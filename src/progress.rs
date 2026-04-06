use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

pub const SAVE_INTERVAL: usize = 50;

#[derive(Serialize, Deserialize, Default)]
pub struct HashCacheEntry {
    pub mtime: u64,
    pub size: u64,
    pub hash: String, // hex-encoded BLAKE3 hash
}

#[derive(Serialize, Deserialize, Default)]
pub struct ProgressState {
    /// Relative paths (as strings) that have been successfully processed
    pub completed: HashSet<String>,
    /// Hash cache keyed by absolute path string
    pub hash_cache: HashMap<String, HashCacheEntry>,
}

impl ProgressState {
    pub fn load_or_default(path: &Path) -> Self {
        if !path.exists() {
            return Self::default();
        }
        let result = std::fs::read_to_string(path)
            .map_err(|e| e.to_string())
            .and_then(|s| serde_json::from_str::<Self>(&s).map_err(|e| e.to_string()));
        match result {
            Ok(state) => state,
            Err(e) => {
                eprintln!("⚠️  Could not parse progress file: {}. Starting fresh.", e);
                Self::default()
            }
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("Failed to create parent dir for {:?}", path))?;
            }
        }
        let content = serde_json::to_string(self).context("Failed to serialize progress state")?;

        // Write to a hidden temp file then rename atomically so a crash mid-write
        // never leaves a corrupted progress file behind.
        let tmp = path.with_file_name(format!(
            ".{}.tmp",
            path.file_name().unwrap_or_default().to_string_lossy()
        ));
        std::fs::write(&tmp, &content)
            .with_context(|| format!("Failed to write temp progress file {:?}", tmp))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("Failed to rename {:?} to {:?}", tmp, path))?;
        Ok(())
    }

    pub fn get_cached_hash(&self, path: &str, mtime: u64, size: u64) -> Option<[u8; 32]> {
        self.hash_cache.get(path).and_then(|entry| {
            if entry.mtime == mtime && entry.size == size {
                blake3::Hash::from_hex(&entry.hash)
                    .ok()
                    .map(|h| *h.as_bytes())
            } else {
                None
            }
        })
    }

    pub fn insert_hash_cache(&mut self, path: String, mtime: u64, size: u64, hash: [u8; 32]) {
        self.hash_cache.insert(
            path,
            HashCacheEntry {
                mtime,
                size,
                hash: blake3::Hash::from_bytes(hash).to_hex().to_string(),
            },
        );
    }
}
