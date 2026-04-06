use anyhow::{Context, Result};
use blake3::Hasher;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use jwalk::{Parallelism, WalkDir};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

const SAVE_INTERVAL: usize = 50;

#[derive(Serialize, Deserialize, Default)]
struct HashCacheEntry {
    mtime: u64,
    size: u64,
    hash: String, // hex-encoded BLAKE3 hash
}

#[derive(Serialize, Deserialize, Default)]
struct ProgressState {
    /// Relative paths (as strings) that have been successfully processed
    completed: HashSet<String>,
    /// Hash cache keyed by absolute path string
    hash_cache: HashMap<String, HashCacheEntry>,
}

impl ProgressState {
    fn load_or_default(path: &Path) -> Self {
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

    fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("Failed to create parent dir for {:?}", path))?;
            }
        }
        let content = serde_json::to_string(self).context("Failed to serialize progress state")?;
        std::fs::write(path, content)
            .with_context(|| format!("Failed to write progress file {:?}", path))?;
        Ok(())
    }

    fn get_cached_hash(&self, path: &str, mtime: u64, size: u64) -> Option<[u8; 32]> {
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

    fn insert_hash_cache(&mut self, path: String, mtime: u64, size: u64, hash: [u8; 32]) {
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

#[derive(Parser)]
#[command(
    name = "awoo",
    about = "High-performance Btrfs merge with BLAKE3 & parallel I/O"
)]
struct Args {
    /// Source directories in format Name:/absolute/path
    #[arg(required = true)]
    sources: Vec<String>,

    /// Output directory for unique consolidated files
    #[arg(short = 'o', long, default_value = "./Consolidated")]
    consolidated: PathBuf,

    /// Output directory for conflicting files
    #[arg(short = 'c', long, default_value = "./Collision")]
    collision: PathBuf,

    /// Show what would be done without copying
    #[arg(long)]
    dry_run: bool,

    /// Resume from a previous interrupted run, skipping already-processed files
    #[arg(long)]
    resume: bool,

    /// Path to the progress/cache file (default: <consolidated>/.awoo_progress.json)
    #[arg(long)]
    progress_file: Option<PathBuf>,
}

struct FileEntry {
    source_name: String,
    abs_path: PathBuf,
    rel_path: PathBuf,
    hash: [u8; 32],
}

/// Copy file using native Btrfs FICLONE ioctl (Linux only)
/// Falls back to std::fs::copy if reflink is not supported
#[cfg(target_os = "linux")]
fn reflink_file(src: &Path, dst: &Path) -> Result<()> {
    use std::fs::OpenOptions;
    use std::os::unix::io::AsRawFd;

    // FICLONE = _IOW(0x94, 9, int) = 0x40049409
    // Stable Linux UAPI constant: https://github.com/torvalds/linux/blob/master/include/uapi/linux/fs.h#L335
    const FICLONE: libc::c_ulong = 0x40049409;

    let src_file = File::open(src).with_context(|| format!("Failed to open source {:?}", src))?;

    // Destination must be a newly created, empty file for FICLONE to work
    let dst_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst)
        .with_context(|| format!("Failed to create destination {:?}", dst))?;

    // SAFETY: ioctl with FICLONE is safe when both FDs are valid, open files.
    // The kernel validates the arguments and returns an error if invalid.
    let ret = unsafe { libc::ioctl(dst_file.as_raw_fd(), FICLONE, src_file.as_raw_fd()) };

    if ret < 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("FICLONE ioctl failed for {:?}", src));
    }

    Ok(())
}

/// Cross-platform wrapper: tries reflink on Linux, falls back to std::fs::copy
fn copy_with_reflink_fallback(src: &Path, dst: &Path) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        // Try native reflink first
        if reflink_file(src, dst).is_ok() {
            return Ok(());
        }
        // If reflink fails (e.g., not on Btrfs, or filesystem doesn't support it),
        // fall through to regular copy below
    }

    // Fallback: standard copy (no reflink)
    std::fs::copy(src, dst).with_context(|| format!("Failed to copy {:?} to {:?}", src, dst))?;
    Ok(())
}

/// Creates `path` as a BTRFS subvolume if possible, otherwise falls back to a regular directory.
/// Does nothing if `path` already exists.
#[cfg(target_os = "linux")]
fn create_subvol_or_dir(path: &Path) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    if path.exists() {
        return Ok(());
    }

    // Ensure the parent directory exists before we can create anything inside it
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)
        .with_context(|| format!("Failed to create parent directory {:?}", parent))?;

    let name = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("Path {:?} has no file name component", path))?;
    let name_lossy = name.to_string_lossy();
    let name_bytes = name_lossy.as_bytes();
    if name_bytes.len() > 4087 {
        anyhow::bail!("Name too long for BTRFS subvolume: {:?}", name);
    }

    // Open the parent directory to obtain a file descriptor for the ioctl
    let parent_file = File::open(parent)
        .with_context(|| format!("Failed to open parent directory {:?}", parent))?;

    // BTRFS_IOC_SUBVOL_CREATE = _IOW(0x94, 14, struct btrfs_ioctl_vol_args)
    // struct btrfs_ioctl_vol_args = { __s64 fd; char name[4088]; } = 4096 bytes
    // _IOW(type, nr, size) = (IOC_WRITE=1 << 30) | (type << 8) | nr | (size << 16)
    //                      = 0x40000000 | 0x9400 | 0x0E | 0x10000000 = 0x5000_940E
    const BTRFS_IOC_SUBVOL_CREATE: libc::c_ulong = 0x5000_940E;

    #[repr(C)]
    struct BtrfsIoctlVolArgs {
        fd: i64,
        name: [u8; 4088],
    }

    let mut vol_args = BtrfsIoctlVolArgs {
        fd: 0,
        name: [0u8; 4088],
    };
    vol_args.name[..name_bytes.len()].copy_from_slice(name_bytes);

    // SAFETY: BTRFS_IOC_SUBVOL_CREATE reads vol_args from userspace to create a subvolume
    // named `name` inside the directory referenced by the fd. The kernel validates both the
    // fd and the name, returning ENOTTY or EINVAL if the filesystem is not Btrfs.
    let ret = unsafe {
        libc::ioctl(
            parent_file.as_raw_fd(),
            BTRFS_IOC_SUBVOL_CREATE,
            &mut vol_args as *mut BtrfsIoctlVolArgs,
        )
    };

    if ret == 0 {
        eprintln!("  🌿 Created BTRFS subvolume: {}", path.display());
        return Ok(());
    }

    // ioctl failed (not a Btrfs volume, or unsupported kernel) — fall back to a plain directory
    std::fs::create_dir_all(path)
        .with_context(|| format!("Failed to create directory {:?}", path))?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn create_subvol_or_dir(path: &Path) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    std::fs::create_dir_all(path)
        .with_context(|| format!("Failed to create directory {:?}", path))?;
    Ok(())
}

/// Hash a file using BLAKE3 with a 256KB read buffer
fn hash_file(path: &Path) -> Result<[u8; 32]> {
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

fn hash_file_cached(path: &Path, state: &Arc<Mutex<ProgressState>>) -> Result<[u8; 32]> {
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

/// Copy file using Btrfs reflink if available (native ioctl), with fallback
fn copy_file(entry: &FileEntry, dst_base: &Path, dry_run: bool) -> Result<()> {
    let dst = dst_base.join(&entry.rel_path);

    if dry_run {
        eprintln!("[DRY] {} -> {}", entry.abs_path.display(), dst.display());
        return Ok(());
    }

    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory {:?}", parent))?;
    }

    // Use native reflink with fallback to regular copy
    copy_with_reflink_fallback(&entry.abs_path, &dst)?;

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Determine progress file path
    let progress_file = args
        .progress_file
        .clone()
        .unwrap_or_else(|| args.consolidated.join(".awoo_progress.json"));

    // Load progress state (hash cache always; completed set only when resuming)
    let state = {
        let has_file = progress_file.exists();
        let mut s = ProgressState::load_or_default(&progress_file);
        if args.resume {
            if has_file {
                eprintln!(
                    "🔄 Resuming: {} previously completed paths, {} cached hashes.\n",
                    s.completed.len(),
                    s.hash_cache.len()
                );
            } else {
                eprintln!(
                    "⚠️  --resume specified but no progress file found at {:?}. Starting fresh.\n",
                    progress_file
                );
            }
        } else {
            // Fresh run: keep hash cache for speed but reset completion records
            s.completed.clear();
        }
        Arc::new(Mutex::new(s))
    };

    // Parse & canonicalize sources
    let mut sources = Vec::new();
    for src in &args.sources {
        let Some((name, path)) = src.split_once(':') else {
            anyhow::bail!("Invalid format '{}'. Expected Name:/path/to/dir", src);
        };
        sources.push((name.to_string(), std::fs::canonicalize(path)?));
    }

    if !args.dry_run {
        create_subvol_or_dir(&args.consolidated).context("Failed to create consolidated dir")?;
        create_subvol_or_dir(&args.collision).context("Failed to create collision dir")?;
    }

    // ─────────────────────────────────────────────────────────────
    // Phase 1: Parallel Scan
    // ─────────────────────────────────────────────────────────────
    let scan_pb = ProgressBar::new_spinner();
    scan_pb.set_style(ProgressStyle::with_template("{spinner:.green} {msg}").unwrap());
    scan_pb.set_message("🔍 Scanning filesystem in parallel...");

    let all_paths: Vec<(String, PathBuf, PathBuf)> = sources
        .iter()
        .flat_map(|(name, src)| {
            let name_clone = name.clone();
            let src_clone = src.clone();
            WalkDir::new(&src_clone)
                .parallelism(Parallelism::RayonDefaultPool {
                    busy_timeout: Duration::new(5, 0),
                })
                .into_iter()
                .filter_map(move |entry| {
                    let entry = entry.ok()?;
                    if !entry.file_type().is_file() {
                        return None;
                    }
                    let rel = entry.path().strip_prefix(&src_clone).unwrap().to_path_buf();
                    Some((name_clone.clone(), entry.path().to_path_buf(), rel))
                })
        })
        .collect();

    scan_pb.finish_and_clear();
    eprintln!(
        "📊 Found {} files across {} sources.\n",
        all_paths.len(),
        sources.len()
    );

    // ─────────────────────────────────────────────────────────────
    // Phase 2: Parallel Hashing (with cache + resume skip)
    // ─────────────────────────────────────────────────────────────
    let hash_pb = ProgressBar::new(all_paths.len() as u64);
    hash_pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.blue} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} {msg}",
        )
        .unwrap(),
    );
    hash_pb.set_message("⚡ Hashing with BLAKE3...");

    let all_entries: Vec<FileEntry> = all_paths
        .par_iter()
        .filter_map(|(name, abs, rel)| {
            // In resume mode, skip files whose rel_path was already completed
            if args.resume {
                let rel_str = rel.to_string_lossy().to_string();
                if state.lock().unwrap().completed.contains(&rel_str) {
                    hash_pb.inc(1);
                    return None;
                }
            }

            match hash_file_cached(abs, &state) {
                Ok(hash) => {
                    hash_pb.inc(1);
                    Some(FileEntry {
                        source_name: name.clone(),
                        abs_path: abs.clone(),
                        rel_path: rel.clone(),
                        hash,
                    })
                }
                Err(e) => {
                    eprintln!("⚠️  Hash error {}: {}", abs.display(), e);
                    hash_pb.inc(1);
                    None
                }
            }
        })
        .collect();

    hash_pb.finish_and_clear();
    eprintln!("✅ Hashed {} files successfully.\n", all_entries.len());

    // Persist hash cache after hashing phase
    if !args.dry_run {
        if let Err(e) = state.lock().unwrap().save(&progress_file) {
            eprintln!("⚠️  Failed to save hash cache: {}", e);
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Phase 3: Group & Parallel Copy/Reflink
    // ─────────────────────────────────────────────────────────────
    let mut db: HashMap<PathBuf, Vec<FileEntry>> = HashMap::new();
    for entry in all_entries {
        db.entry(entry.rel_path.clone()).or_default().push(entry);
    }

    let copy_pb = ProgressBar::new(db.len() as u64);
    copy_pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.blue} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} {msg}",
        )
        .unwrap(),
    );
    copy_pb.set_message("📦 Copying & deduplicating...");

    db.par_iter().for_each(|(rel_path, entries)| {
        let rel_str = rel_path.to_string_lossy().to_string();
        let unique_hashes: HashSet<[u8; 32]> = entries.iter().map(|e| e.hash).collect();

        let success = if unique_hashes.len() == 1 {
            copy_file(&entries[0], &args.consolidated, args.dry_run)
                .map_err(|e| eprintln!("⚠️  Consolidate failed: {}", e))
                .is_ok()
        } else {
            entries.iter().all(|entry| {
                let dst_base = args.collision.join(&entry.source_name);
                copy_file(entry, &dst_base, args.dry_run)
                    .map_err(|e| eprintln!("⚠️  Collision copy failed: {}", e))
                    .is_ok()
            })
        };

        if success && !args.dry_run {
            let should_save = {
                let mut s = state.lock().unwrap();
                s.completed.insert(rel_str);
                s.completed.len() % SAVE_INTERVAL == 0
            };
            if should_save {
                if let Err(e) = state.lock().unwrap().save(&progress_file) {
                    eprintln!("⚠️  Failed to save progress checkpoint: {}", e);
                }
            }
        }

        copy_pb.inc(1);
    });

    copy_pb.finish_and_clear();

    // Final save
    if !args.dry_run {
        if let Err(e) = state.lock().unwrap().save(&progress_file) {
            eprintln!("⚠️  Failed to save final progress: {}", e);
        }
    }

    // Summary
    let cons_count = db
        .iter()
        .filter(|(_, v)| v.iter().map(|e| e.hash).collect::<HashSet<_>>().len() == 1)
        .count();
    let coll_count = db
        .iter()
        .filter(|(_, v)| v.iter().map(|e| e.hash).collect::<HashSet<_>>().len() > 1)
        .map(|(_, v)| v.len())
        .sum::<usize>();
    let total_completed = state.lock().unwrap().completed.len();

    eprintln!("\n🏁 Done!");
    eprintln!("  📂 Consolidated: {} unique paths", cons_count);
    eprintln!("  💥 Collisions:   {} files", coll_count);
    if args.resume && total_completed > cons_count + coll_count {
        eprintln!(
            "  ⏭️  Skipped (already done): {} paths",
            total_completed.saturating_sub(cons_count + coll_count)
        );
    }
    eprintln!("  📊 Total completed: {} paths", total_completed);
    Ok(())
}
