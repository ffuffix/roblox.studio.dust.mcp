//! Filesystem rendezvous between the adapter and the broker.
//!
//! The broker records its `{port, pid, token, ...}` in `~/.dust/broker.json`
//! on startup. Adapters read it to discover and authenticate to a running
//! broker, or detect its absence and fork one. Plugins do *not* use this — they
//! cannot read files and discover the broker over HTTP instead.

use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::protocol::BrokerInfo;

pub fn dust_dir() -> Result<PathBuf> {
    let home = dirs::home_dir().context("could not determine home directory")?;
    Ok(home.join(".dust"))
}

pub fn broker_json_path() -> Result<PathBuf> {
    Ok(dust_dir()?.join("broker.json"))
}

pub fn write_broker_info(info: &BrokerInfo) -> Result<()> {
    let dir = dust_dir()?;
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    let path = broker_json_path()?;
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_vec_pretty(info)?;
    std::fs::write(&tmp, &json).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("renaming into {}", path.display()))?;
    Ok(())
}

pub fn read_broker_info() -> Option<BrokerInfo> {
    let path = broker_json_path().ok()?;
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

pub fn remove_broker_info_if_owned(broker_uuid: &str) {
    if read_broker_info().is_some_and(|info| info.broker_uuid == broker_uuid)
        && let Ok(path) = broker_json_path()
    {
        let _ = std::fs::remove_file(path);
    }
}
