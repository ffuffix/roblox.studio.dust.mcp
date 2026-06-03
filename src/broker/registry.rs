//! The broker's single source of truth: the session registry, per-role command
//! queues, and the correlation table that lets an adapter await a plugin's
//! result.
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

const REDELIVER_AFTER: Duration = Duration::from_secs(15);
pub const STALE_AFTER: Duration = Duration::from_secs(40);
pub const DEAD_AFTER: Duration = Duration::from_secs(120);

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

pub struct RoleQueue {
    pub notify: Notify,
    inner: Mutex<RoleQueueInner>,
}

struct RoleQueueInner {
    last_seen: Instant,
    undelivered: VecDeque<Command>,
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

    pub fn touch(&self) {
        self.inner.lock().unwrap().last_seen = Instant::now();
    }

    fn last_seen(&self) -> Instant {
        self.inner.lock().unwrap().last_seen
    }

    pub fn enqueue(&self, cmd: Command) {
        self.inner.lock().unwrap().undelivered.push_back(cmd);
        self.notify.notify_waiters();
    }

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

    pub fn ack(&self, id: u64) {
        let mut guard = self.inner.lock().unwrap();
        guard.inflight.remove(&id);
        guard.undelivered.retain(|c| c.id != id);
    }
}

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

    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn role_queue(&self, role: Role) -> Arc<RoleQueue> {
        self.roles
            .entry(role)
            .or_insert_with(|| Arc::new(RoleQueue::new(Instant::now())))
            .clone()
    }

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

    fn has_live_role(&self) -> bool {
        let now = Instant::now();
        self.roles
            .iter()
            .any(|e| now.duration_since(e.value().last_seen()) < STALE_AFTER)
    }

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

#[derive(Default)]
pub struct Registry {
    sessions: DashMap<String, Arc<Session>>,
    pending: DashMap<(String, u64), oneshot::Sender<CommandResult>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

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

    pub fn live_session_count(&self) -> usize {
        self.sessions
            .iter()
            .filter(|e| e.value().has_live_role())
            .count()
    }

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

    pub fn register_pending(&self, session_id: &str, id: u64) -> oneshot::Receiver<CommandResult> {
        let (tx, rx) = oneshot::channel();
        self.pending.insert((session_id.to_string(), id), tx);
        rx
    }

    pub fn cancel_pending(&self, session_id: &str, id: u64) {
        self.pending.remove(&(session_id.to_string(), id));
    }

    pub fn complete(&self, session_id: &str, result: CommandResult) -> bool {
        if let Some((_, tx)) = self.pending.remove(&(session_id.to_string(), result.id)) {
            tx.send(result).is_ok()
        } else {
            false
        }
    }
}
