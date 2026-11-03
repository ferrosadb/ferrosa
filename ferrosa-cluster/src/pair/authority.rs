//! Durable pair ownership, independent of connection direction and UUID ordering.
//! Correctness: an acknowledged ownership change survives restart; invalid records fail closed.
//! Last revised: 2026-10-29
//! Last changed: Persist the ownership assignment before sending a handoff grant.

use std::io::{Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{ClusterError, Result};

const MARKER: &str = "pair-authority.json";
const MAX_MARKER_BYTES: u64 = 4096;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PairAuthority {
    pub version: u32,
    pub local: Uuid,
    pub peer: Uuid,
    pub primary: Uuid,
    #[serde(default)]
    pub generation: u64,
}

impl PairAuthority {
    fn validate(&self, local: Uuid) -> Result<()> {
        if self.version != 1
            || self.local != local
            || self.local == self.peer
            || (self.primary != self.local && self.primary != self.peer)
        {
            return Err(ClusterError::Internal(
                "invalid pair authority record; writes fenced".into(),
            ));
        }
        Ok(())
    }

    pub fn read(root: &Path, local: Uuid) -> Result<Option<Self>> {
        let file = match std::fs::File::open(root.join(MARKER)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(ClusterError::Internal(format!(
                    "read pair authority: {error}"
                )))
            }
        };
        let mut bytes = Vec::new();
        file.take(MAX_MARKER_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| ClusterError::Internal(format!("read pair authority: {error}")))?;
        if bytes.len() as u64 > MAX_MARKER_BYTES {
            return Err(ClusterError::Internal(
                "oversized pair authority record; writes fenced".into(),
            ));
        }
        let record: Self = serde_json::from_slice(&bytes)
            .map_err(|error| ClusterError::Internal(format!("decode pair authority: {error}")))?;
        record.validate(local)?;
        Ok(Some(record))
    }

    pub fn store(root: &Path, local: Uuid, peer: Uuid, primary: Uuid) -> Result<()> {
        let generation = Self::read(root, local)?
            .map_or(0, |r| r.generation)
            .checked_add(1)
            .ok_or_else(|| ClusterError::Internal("pair authority generation exhausted".into()))?;
        Self::store_generation(root, local, peer, primary, generation)
    }

    pub fn store_generation(
        root: &Path,
        local: Uuid,
        peer: Uuid,
        primary: Uuid,
        generation: u64,
    ) -> Result<()> {
        let record = Self {
            version: 1,
            local,
            peer,
            primary,
            generation,
        };
        record.validate(local)?;
        let io = |error| ClusterError::Internal(format!("persist pair authority: {error}"));
        std::fs::create_dir_all(root).map_err(io)?;
        let staging = root.join(format!("{MARKER}.{}.partial", Uuid::new_v4()));
        let bytes = serde_json::to_vec(&record)
            .map_err(|error| ClusterError::Internal(format!("encode pair authority: {error}")))?;
        let result = (|| {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staging)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&staging, root.join(MARKER))?;
            std::fs::File::open(root)?.sync_all()
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&staging);
        }
        result.map_err(io)
    }
}
