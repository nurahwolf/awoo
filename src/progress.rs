use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// How often (in completed paths) to write a Phase 3 checkpoint.
/// Each checkpoint only writes the `completed` set — not the hash cache —
/// so this can be kept low without any serialisation overhead.
pub const SAVE_INTERVAL: usize = 500;

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
            // No main progress file — check whether there is an orphaned checkpoint
            // (e.g. from a previous run that was killed before the final full save).
            let mut state = Self::default();
            Self::merge_checkpoint(path, &mut state.completed);
            return state;
        }
        let result = std::fs::read_to_string(path)
            .map_err(|e| e.to_string())
            .and_then(|s| serde_json::from_str::<Self>(&s).map_err(|e| e.to_string()));
        let mut state = match result {
            Ok(s) => s,
            Err(e) => {
                eprintln!("⚠️  Could not parse progress file: {}. Starting fresh.", e);
                Self::default()
            }
        };
        // Merge any Phase 3 checkpoint that was written after the last full save.
        Self::merge_checkpoint(path, &mut state.completed);
        state
    }

    /// Serialise the full state (hash cache + completed set) to `path`.
    ///
    /// Called once after Phase 2 (to persist the hash cache) and once at the
    /// very end of Phase 3 (to persist the final completed set).  This write
    /// can be large — O(number of files × average path length) — so it must
    /// not be called on the hot copy path.
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

    /// Serialise **only** the `completed` set to a lightweight checkpoint file,
    /// deliberately omitting the hash cache.
    ///
    /// The hash cache does not change during Phase 3 (no new files are hashed),
    /// so there is no need to re-serialise it on every checkpoint.  Omitting it
    /// reduces a potential 200 MB+ JSON write down to a few hundred kilobytes,
    /// eliminating the per-checkpoint stall that was serialising the entire
    /// Rayon thread pool through a multi-second mutex hold.
    ///
    /// The checkpoint file is a sibling of `path` with a `.ckpt` extension.
    /// `load_or_default` automatically merges it back on the next `--resume`.
    pub fn save_completions(&self, path: &Path) -> Result<()> {
        #[derive(serde::Serialize)]
        struct Slim<'a> {
            completed: &'a std::collections::HashSet<String>,
        }

        let content = serde_json::to_string(&Slim {
            completed: &self.completed,
        })
        .context("Failed to serialize completions checkpoint")?;

        let ckpt = ckpt_path(path);
        let tmp = ckpt.with_file_name(format!(
            ".{}.tmp",
            ckpt.file_name().unwrap_or_default().to_string_lossy()
        ));
        std::fs::write(&tmp, &content)
            .with_context(|| format!("Failed to write completions checkpoint {:?}", tmp))?;
        std::fs::rename(&tmp, &ckpt)
            .with_context(|| format!("Failed to rename {:?} to {:?}", tmp, ckpt))?;
        Ok(())
    }

    /// Merge any pending lightweight checkpoint back into this state and
    /// delete the checkpoint file.  Called by `load_or_default` automatically.
    fn merge_checkpoint(path: &Path, completed: &mut std::collections::HashSet<String>) {
        #[derive(serde::Deserialize)]
        struct Slim {
            completed: std::collections::HashSet<String>,
        }

        let ckpt = ckpt_path(path);
        if !ckpt.exists() {
            return;
        }
        let merged = std::fs::read_to_string(&ckpt)
            .map_err(|e| e.to_string())
            .and_then(|s| serde_json::from_str::<Slim>(&s).map_err(|e| e.to_string()));

        match merged {
            Ok(slim) => {
                let added = slim.completed.len();
                completed.extend(slim.completed);
                eprintln!("  + Merged {} completions from checkpoint.", added);
                let _ = std::fs::remove_file(&ckpt);
            }
            Err(e) => {
                eprintln!(
                    "⚠️  Could not read completions checkpoint: {}. Ignoring.",
                    e
                );
            }
        }
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

/// Returns the path of the lightweight completions checkpoint that lives
/// alongside the main progress file.
///
/// Example: `.awoo_progress.json` → `.awoo_progress.json.ckpt`
pub fn ckpt_path(progress_path: &Path) -> std::path::PathBuf {
    let mut name = progress_path.file_name().unwrap_or_default().to_os_string();
    name.push(".ckpt");
    progress_path.with_file_name(name)
}
