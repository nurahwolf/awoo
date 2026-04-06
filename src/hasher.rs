use anyhow::{Context, Result};
use blake3::Hasher;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

use crate::progress::ProgressState;

/// A 256 KB buffer with the 4096-byte alignment required by `O_DIRECT`.
///
/// `repr(C, align(4096))` guarantees the alignment. The `Box` used at the
/// call site keeps the 256 KB off the stack; in release mode rustc allocates
/// the value directly on the heap without a stack intermediate.
#[cfg(target_os = "linux")]
#[repr(C, align(4096))]
struct DirectBuf([u8; 256 * 1024]);

/// Hash a file using BLAKE3 with a 256 KB read buffer.
///
/// On Linux, opens the file with `O_DIRECT` to bypass the page cache. Each
/// source file is read only once during a run, so caching its pages serves no
/// purpose and needlessly evicts other useful data from RAM. If the filesystem
/// does not support `O_DIRECT` (e.g. tmpfs, NFS) the function silently falls
/// back to ordinary buffered I/O.
pub fn hash_file(path: &Path) -> Result<[u8; 32]> {
    let mut hasher = Hasher::new();

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;

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
        // O_DIRECT not supported on this filesystem — fall through to buffered I/O.
    }

    // Non-Linux, or O_DIRECT unavailable: standard buffered read.
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
/// The lock on `state` is held only briefly around cache reads and writes —
/// never during the actual file I/O.
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
