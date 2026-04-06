use anyhow::{Context, Result};
use blake3::Hasher;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::UNIX_EPOCH;

use crate::progress::ProgressState;

/// Files at or above this size are hashed via a memory map +
/// `blake3::Hasher::update_rayon()`, which distributes the work across all
/// Rayon threads and saturates modern NVMe drives.  Files below this threshold
/// are hashed sequentially with O_DIRECT to avoid cache pollution.
#[cfg(target_os = "linux")]
const LARGE_FILE_THRESHOLD: u64 = 256 * 1024 * 1024; // 256 MB

/// A 256 KB buffer with the 4096-byte alignment required by `O_DIRECT`.
///
/// `repr(C, align(4096))` guarantees the alignment. The `Box` used at the
/// call site keeps the 256 KB off the stack; in release mode rustc allocates
/// the value directly on the heap without a stack intermediate.
#[cfg(target_os = "linux")]
#[repr(C, align(4096))]
struct DirectBuf([u8; 256 * 1024]);

/// Hash a file using BLAKE3.
///
/// **Linux strategy (in order of preference):**
/// 1. Files ≥ 256 MB — memory-mapped + `update_rayon` for multi-core throughput.
/// 2. Smaller files — `O_DIRECT` sequential read to bypass the page cache.
/// 3. Fallback — standard buffered read if `O_DIRECT` is unsupported.
///
/// **Non-Linux:** standard buffered read.
pub fn hash_file(path: &Path) -> Result<[u8; 32]> {
    let mut hasher = Hasher::new();

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let size = std::fs::metadata(path)
            .with_context(|| format!("Failed to stat {:?}", path))?
            .len();

        // ── Large file: mmap + parallel BLAKE3 ──────────────────────────
        if size >= LARGE_FILE_THRESHOLD {
            let file =
                std::fs::File::open(path).with_context(|| format!("Failed to open {:?}", path))?;
            // SAFETY: The file contents are not mutated between the open and the
            // map, and the mapping is not used beyond this function.
            let mmap = unsafe { memmap2::Mmap::map(&file) }
                .with_context(|| format!("Failed to mmap {:?}", path))?;
            hasher.update_rayon(&mmap);
            return Ok(*hasher.finalize().as_bytes());
        }

        // ── Small file: O_DIRECT sequential read ────────────────────────
        let direct = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(path);

        if let Ok(mut file) = direct {
            let mut buf = Box::new(DirectBuf([0u8; 256 * 1024]));
            loop {
                let n = file
                    .read(&mut buf.0)
                    .context("Failed to read file (O_DIRECT)")?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf.0[..n]);
            }
            return Ok(*hasher.finalize().as_bytes());
        }
        // O_DIRECT not supported on this filesystem — fall through.
    }

    // Non-Linux or O_DIRECT unavailable: standard buffered read.
    let mut file = File::open(path).with_context(|| format!("Failed to open {:?}", path))?;
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

/// Hash a file, returning a cached result if `mtime` and `size` are unchanged.
///
/// Uses a **read lock** for cache lookups (multiple threads can look up
/// concurrently) and a **write lock** only on a cache miss to insert the new
/// entry.  The actual file I/O holds no lock at all.
pub fn hash_file_cached(path: &Path, state: &Arc<RwLock<ProgressState>>) -> Result<[u8; 32]> {
    let metadata = std::fs::metadata(path).with_context(|| format!("Failed to stat {:?}", path))?;
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let size = metadata.len();
    let path_key = path.to_string_lossy().to_string();

    // Read lock — concurrent lookups are allowed
    {
        let s = state.read().unwrap();
        if let Some(hash) = s.get_cached_hash(&path_key, mtime, size) {
            return Ok(hash);
        }
    }

    // Compute hash with no lock held
    let hash = hash_file(path)?;

    // Write lock — exclusive insert
    {
        let mut s = state.write().unwrap();
        s.insert_hash_cache(path_key, mtime, size, hash);
    }

    Ok(hash)
}

