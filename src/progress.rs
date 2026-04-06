use ahash::{AHashMap, AHashSet};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

// ── Custom serde for [u8; 32] ─────────────────────────────────────────────
// Stores the hash as a 64-character hex string in JSON (human-readable,
// compatible with the existing on-disk format) but keeps it as raw bytes
// in memory to avoid a hex-decode on every cache lookup.
mod hash_hex {
    use serde::{de::Error as _, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&blake3::Hash::from_bytes(*bytes).to_hex())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
        let s = <&str as serde::Deserialize>::deserialize(d)?;
        blake3::Hash::from_hex(s)
            .map(|h| *h.as_bytes())
            .map_err(D::Error::custom)
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct HashCacheEntry {
    pub mtime: u64,
    pub size: u64,
    /// Raw BLAKE3 digest stored as bytes; serialised as hex in JSON.
    #[serde(with = "hash_hex")]
    pub hash: [u8; 32],
}

#[derive(Serialize, Deserialize, Default)]
pub struct ProgressState {
    /// Relative paths (as strings) that have been successfully processed.
    pub completed: AHashSet<String>,
    /// Hash cache keyed by absolute path string.
    pub hash_cache: AHashMap<String, HashCacheEntry>,
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
        // Merge any Phase 3 checkpoint written after the last full save.
        Self::merge_checkpoint(path, &mut state.completed);
        state
    }

    /// Serialise the full state (hash cache + completed set) to `path`.
    ///
    /// Called once after Phase 2 (to persist the hash cache) and once at the
    /// very end of Phase 3. This write can be large, so it must not be called
    /// on the hot copy path.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("Failed to create parent dir for {:?}", path))?;
            }
        }
        let content = serde_json::to_string(self).context("Failed to serialize progress state")?;

        // Atomic write via temp file + rename.
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

    /// Merge completions from the checkpoint file into `completed`.
    ///
    /// Handles two formats:
    /// - **Old JSON format** (`{"completed":[...]}`) written by older awoo builds.
    ///   Merged and then deleted (it is a one-shot snapshot).
    /// - **New line-delimited format** (one relative path per line) written by the
    ///   current append-only log. Merged but NOT deleted — the file will be
    ///   appended to during the current run and removed only after the final
    ///   full save succeeds.
    fn merge_checkpoint(path: &Path, completed: &mut AHashSet<String>) {
        let ckpt = ckpt_path(path);
        if !ckpt.exists() {
            return;
        }
        let content = match std::fs::read_to_string(&ckpt) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("⚠️  Could not read checkpoint: {}. Ignoring.", e);
                return;
            }
        };

        // Try old JSON format first (backward compatibility).
        #[derive(serde::Deserialize)]
        struct SlimJson {
            completed: AHashSet<String>,
        }
        if let Ok(slim) = serde_json::from_str::<SlimJson>(&content) {
            let added = slim.completed.len();
            completed.extend(slim.completed);
            if added > 0 {
                eprintln!("  + Merged {} completions from checkpoint.", added);
            }
            let _ = std::fs::remove_file(&ckpt); // JSON ckpt is a one-shot; delete it
            return;
        }

        // New line-delimited format.
        let before = completed.len();
        for line in content.lines() {
            let line = line.trim();
            if !line.is_empty() {
                completed.insert(line.to_string());
            }
        }
        let added = completed.len() - before;
        if added > 0 {
            eprintln!("  + Merged {} completions from checkpoint.", added);
        }
        // Do NOT delete — will be appended to and removed after a successful
        // final full save.
    }

    /// Look up a cached hash. Returns `Some([u8; 32])` if `mtime` and `size`
    /// both match — no hex-decode required, the bytes are copied directly.
    pub fn get_cached_hash(&self, path: &str, mtime: u64, size: u64) -> Option<[u8; 32]> {
        self.hash_cache.get(path).and_then(|entry| {
            if entry.mtime == mtime && entry.size == size {
                Some(entry.hash) // Direct [u8; 32] copy — zero decode overhead
            } else {
                None
            }
        })
    }

    pub fn insert_hash_cache(&mut self, path: String, mtime: u64, size: u64, hash: [u8; 32]) {
        self.hash_cache
            .insert(path, HashCacheEntry { mtime, size, hash });
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
