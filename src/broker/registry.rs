//! The broker's single source of truth: the session registry, per-role command
//! queues, and the correlation table that lets an adapter await a plugin's
//! result (§1, §3, §4).
//!
//! Concurrency rules followed throughout:
//! - The `DashMap`s are sharded locks; we never hold a `Ref`/`RefMut` across an
//!   `.await`. The long-poll handler clones the `Arc<RoleQueue>` out and drops
//!   the map ref before awaiting.
//! - Each `RoleQueue` guards its state with a *std* `Mutex` held only for short,
//!   synchronous critical sections — never across `.await`. Blocking is done on
//!   the queue's `Notify`, outside the lock.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::{Notify, oneshot};

use crate::protocol::{
    Command, CommandResult, Handshake, LiveState, Role, RoleInfo, SessionInfo,
};

/// A delivered-but-unacked command is redelivered on a later poll if it has
/// been outstanding this long. Handles a lost poll response or a reconnect
/// without busy-looping on freshly delivered work (the plugin dedups by id).
const REDELIVER_AFTER: Duration = Duration::from_secs(15);
/// A role unseen for longer than this is `Stale`.
pub const STALE_AFTER: Duration = Duration::from_secs(40);
/// A role unseen for longer than this is `Dead` and eligible for reaping.
pub const DEAD_AFTER: Duration = Duration::from_secs(120);

/// Mutable per-session metadata, refreshed from each handshake.
#[derive(Clone, Debug, Default)]
pub struct SessionMeta {
    pub place_id: u64,
    pub game_id: u64,
    pub place_name: String,
    pub creator_id: u64,
    pub label: Option<String>,
}

impl SessionMeta {
    fn from_handshake(hs: &Handshake) -> Self {
        Self {
            place_id: hs.place_id,
            game_id: hs.game_id,
            place_name: hs.place_name.clone(),
            creator_id: hs.creator_id,
            label: hs.label.clone(),
        }
    }

    /// Merge a fresh handshake in. A handshake never clears an existing label
    /// with `None`, so a label assigned once sticks.
    fn update_from(&mut self, hs: &Handshake) {
        self.place_id = hs.place_id;
        self.game_id = hs.game_id;
        if !hs.place_name.is_empty() {
            self.place_name = hs.place_name.clone();
        }
        self.creator_id = hs.creator_id;
        if hs.label.is_some() {
            self.label = hs.label.clone();
        }
    }
}

/// The FIFO command queue for one role of one session, with at-least-once
/// delivery and idempotent acks (§4).
pub struct RoleQueue {
    /// Woken whenever a command is enqueued, so a parked long-poll re-checks.
    pub notify: Notify,
    inner: Mutex<RoleQueueInner>,
}

struct RoleQueueInner {
    last_seen: Instant,
    /// Never-delivered commands, in FIFO order.
    undelivered: VecDeque<Command>,
    /// Delivered-but-unacked commands with the time they were last handed out,
    /// keyed by id for ordered iteration and O(log n) ack.
    inflight: BTreeMap<u64, (Command, Instant)>,
}

impl RoleQueue {
    fn new(now: Instant) -> Self {
        Self {
            notify: Notify::new(),
            inner: Mutex::new(RoleQueueInner {
                last_seen: now,
                undelivered: VecDeque::new(),
                inflight: BTreeMap::new(),
            }),
        }
    }

    /// Record a heartbeat for this role.
    pub fn touch(&self) {
        self.inner.lock().unwrap().last_seen = Instant::now();
    }

    fn last_seen(&self) -> Instant {
        self.inner.lock().unwrap().last_seen
    }

    /// Queue a command and wake any parked poll.
    pub fn enqueue(&self, cmd: Command) {
        self.inner.lock().unwrap().undelivered.push_back(cmd);
        // notify_waiters does not store a permit, but that is fine: a poll that
        // arrives later re-checks the queue before parking, so no wakeup is lost.
        self.notify.notify_waiters();
    }

    /// Drain the work to return for a poll: every never-delivered command (now
    /// moved to in-flight) plus any in-flight command stale enough to redeliver.
    /// Returns commands sorted by id with no duplicates.
    pub fn take_for_poll(&self) -> Vec<Command> {
        let now = Instant::now();
        let mut guard = self.inner.lock().unwrap();
        let mut out: Vec<Command> = Vec::new();

        while let Some(cmd) = guard.undelivered.pop_front() {
            out.push(cmd.clone());
            guard.inflight.insert(cmd.id, (cmd, now));
        }
        for (cmd, delivered_at) in guard.inflight.values() {
            if now.duration_since(*delivered_at) >= REDELIVER_AFTER {
                out.push(cmd.clone());
            }
        }

        out.sort_by_key(|c| c.id);
        out.dedup_by_key(|c| c.id);
        out
    }

    /// Mark a command acknowledged (idempotent — a duplicate ack is a no-op).
    pub fn ack(&self, id: u64) {
        let mut guard = self.inner.lock().unwrap();
        guard.inflight.remove(&id);
        guard.undelivered.retain(|c| c.id != id);
    }
}

/// One place launch, grouping up to three role queues that share a `sessionId`.
pub struct Session {
    pub meta: Mutex<SessionMeta>,
    next_id: AtomicU64,
    roles: DashMap<Role, Arc<RoleQueue>>,
}

impl Session {
    fn new(meta: SessionMeta) -> Self {
        Self {
            meta: Mutex::new(meta),
            next_id: AtomicU64::new(1),
            roles: DashMap::new(),
        }
    }

    /// Allocate the next monotonic command id for this session.
    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get or create the queue for a role.
    pub fn role_queue(&self, role: Role) -> Arc<RoleQueue> {
        self.roles
            .entry(role)
            .or_insert_with(|| Arc::new(RoleQueue::new(Instant::now())))
            .clone()
    }

    /// Route a command to its target role's queue.
    pub fn enqueue(&self, cmd: Command) {
        self.role_queue(cmd.target_role).enqueue(cmd);
    }

    fn role_infos(&self) -> Vec<RoleInfo> {
        let now = Instant::now();
        let mut infos: Vec<RoleInfo> = self
            .roles
            .iter()
            .map(|e| {
                let ago = now.duration_since(e.value().last_seen());
                RoleInfo {
                    role: *e.key(),
                    state: state_for(ago),
                    last_seen_ms: ago.as_millis() as u64,
                }
            })
            .collect();
        infos.sort_by_key(|r| format!("{:?}", r.role));
        infos
    }

    /// `true` if any role has been seen recently enough to count as live.
    fn has_live_role(&self) -> bool {
        let now = Instant::now();
        self.roles
            .iter()
            .any(|e| now.duration_since(e.value().last_seen()) < STALE_AFTER)
    }

    /// `true` if every role is dead (or there are none) — eligible for reaping.
    fn all_dead(&self) -> bool {
        let now = Instant::now();
        self.roles
            .iter()
            .all(|e| now.duration_since(e.value().last_seen()) >= DEAD_AFTER)
    }
}

fn state_for(ago: Duration) -> LiveState {
    if ago < STALE_AFTER {
        LiveState::Live
    } else if ago < DEAD_AFTER {
        LiveState::Stale
    } else {
        LiveState::Dead
    }
}

/// The broker-wide registry.
#[derive(Default)]
pub struct Registry {
    sessions: DashMap<String, Arc<Session>>,
    /// Adapters awaiting a result, keyed by `(sessionId, commandId)`.
    pending: DashMap<(String, u64), oneshot::Sender<CommandResult>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register or refresh a session+role from a handshake, returning the
    /// session. Records the heartbeat for the role.
    pub fn upsert(&self, hs: &Handshake) -> Arc<Session> {
        let session = self
            .sessions
            .entry(hs.session_id.clone())
            .or_insert_with(|| Arc::new(Session::new(SessionMeta::from_handshake(hs))))
            .clone();
        session.meta.lock().unwrap().update_from(hs);
        session.role_queue(hs.role).touch();
        session
    }

    pub fn get(&self, session_id: &str) -> Option<Arc<Session>> {
        self.sessions.get(session_id).map(|e| e.value().clone())
    }

    /// Snapshot of all sessions for `list_sessions`.
    pub fn list(&self) -> Vec<SessionInfo> {
        self.sessions
            .iter()
            .map(|e| {
                let session = e.value();
                let meta = session.meta.lock().unwrap().clone();
                SessionInfo {
                    session_id: e.key().clone(),
                    place_id: meta.place_id,
                    game_id: meta.game_id,
                    place_name: meta.place_name,
                    creator_id: meta.creator_id,
                    label: meta.label,
                    roles: session.role_infos(),
                }
            })
            .collect()
    }

    /// Count sessions with at least one live role (for idle-shutdown).
    pub fn live_session_count(&self) -> usize {
        self.sessions
            .iter()
            .filter(|e| e.value().has_live_role())
            .count()
    }

    /// Drop sessions whose every role is dead. Returns the number removed.
    pub fn reap(&self) -> usize {
        let dead: Vec<String> = self
            .sessions
            .iter()
            .filter(|e| e.value().all_dead())
            .map(|e| e.key().clone())
            .collect();
        for id in &dead {
            self.sessions.remove(id);
        }
        dead.len()
    }

    /// Register interest in a command's result, returning the receiver to await.
    pub fn register_pending(&self, session_id: &str, id: u64) -> oneshot::Receiver<CommandResult> {
        let (tx, rx) = oneshot::channel();
        self.pending.insert((session_id.to_string(), id), tx);
        rx
    }

    /// Abandon interest in a result (e.g. on adapter timeout).
    pub fn cancel_pending(&self, session_id: &str, id: u64) {
        self.pending.remove(&(session_id.to_string(), id));
    }

    /// Deliver a result to any waiting adapter. Returns `true` if one was waiting.
    pub fn complete(&self, session_id: &str, result: CommandResult) -> bool {
        if let Some((_, tx)) = self.pending.remove(&(session_id.to_string(), result.id)) {
            tx.send(result).is_ok()
        } else {
            false
        }
    }
}
