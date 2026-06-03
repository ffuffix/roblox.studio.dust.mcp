//! Dust — a reliable Roblox Studio MCP.
//!
//! The crate is built around a broker-daemon topology:
//!
//! - [`protocol`] — the wire schema shared by every process.
//! - [`discovery`] — adapter↔broker filesystem rendezvous (`broker.json`).
//! - [`broker`] — the daemon that owns the port, registry, and routing.
//! - [`adapter`] — the stdio MCP server Claude launches; bridges to the broker.
//!
//! The Studio plugin (Luau) lives under `plugin/` and speaks the same
//! [`protocol`] over HTTP.

pub mod adapter;
pub mod broker;
pub mod discovery;
pub mod protocol;
