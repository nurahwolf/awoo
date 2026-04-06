use anyhow::{Context, Result};
use blake3::Hasher;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

use crate::progress::ProgressState;

/// Hash a file using BLAKE3 with a 256KB read buffer
pub fn hash_file(path: &Path) -> Result<[u8; 32]> {
    let mut file = File::open(path).with_context(|| format!("Failed to open {:?}", path))?;
    let mut hasher = Hasher::new();
    let mut buffer = [0u8; 256 * 1024];

    loop {
        let n = file.read(&mut buffer).context("Failed to read file")?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok(*hasher.finalize().as_bytes())
}

pub fn hash_file_cached(path: &Path, state: &Arc<Mutex<ProgressState>>) -> Result<[u8; 32]> {
    let metadata = std::fs::metadata(path).with_context(|| format!("Failed to stat {:?}", path))?;
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let size = metadata.len();
    let path_key = path.to_string_lossy().to_string();

    // Check cache (brief lock)
    {
        let s = state.lock().unwrap();
        if let Some(hash) = s.get_cached_hash(&path_key, mtime, size) {
            return Ok(hash);
        }
    }

    // Compute hash (no lock held during I/O)
    let hash = hash_file(path)?;

    // Store in cache (brief lock)
    {
        let mut s = state.lock().unwrap();
        s.insert_hash_cache(path_key, mtime, size, hash);
    }

    Ok(hash)
}
