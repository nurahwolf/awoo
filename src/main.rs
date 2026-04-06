use anyhow::{Context, Result};
use clap::Parser;
use blake3::Hasher;
use jwalk::{WalkDir, Parallelism};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;
use indicatif::{ProgressBar, ProgressStyle};

#[derive(Parser)]
#[command(name = "btrfs_merge", about = "High-performance Btrfs merge with BLAKE3 & parallel I/O")]
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
    use std::os::unix::io::AsRawFd;
    use std::fs::OpenOptions;

    // FICLONE = _IOW(0x94, 9, int) = 0x40049409
    // Stable Linux UAPI constant: https://github.com/torvalds/linux/blob/master/include/uapi/linux/fs.h#L335
    const FICLONE: libc::c_ulong = 0x40049409;
    
    let src_file = File::open(src)
        .with_context(|| format!("Failed to open source {:?}", src))?;

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
    std::fs::copy(src, dst)
        .with_context(|| format!("Failed to copy {:?} to {:?}", src, dst))?;
    Ok(())
}

/// Hash a file using BLAKE3 with a 256KB read buffer
fn hash_file(path: &Path) -> Result<[u8; 32]> {
    let mut file = File::open(path).with_context(|| format!("Failed to open {:?}", path))?;
    let mut hasher = Hasher::new();
    let mut buffer = [0u8; 256 * 1024];
    
    loop {
        let n = file.read(&mut buffer).context("Failed to read file")?;
        if n == 0 { break; }
        hasher.update(&buffer[..n]);
    }
    Ok(*hasher.finalize().as_bytes())
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

    // Parse & canonicalize sources
    let mut sources = Vec::new();
    for src in &args.sources {
        let Some((name, path)) = src.split_once(':') else {
            anyhow::bail!("Invalid format '{}'. Expected Name:/path/to/dir", src);
        };
        sources.push((name.to_string(), std::fs::canonicalize(path)?));
    }

    if !args.dry_run {
        std::fs::create_dir_all(&args.consolidated).context("Failed to create consolidated dir")?;
        std::fs::create_dir_all(&args.collision).context("Failed to create collision dir")?;
    }

    // ─────────────────────────────────────────────────────────────
    // Phase 1: Parallel Scan (Indeterminate Spinner)
    // ─────────────────────────────────────────────────────────────
    let scan_pb = ProgressBar::new_spinner();
    scan_pb.set_style(ProgressStyle::with_template("{spinner:.green} {msg}").unwrap());
    scan_pb.set_message("🔍 Scanning filesystem in parallel...");

    let all_paths: Vec<(String, PathBuf, PathBuf)> = sources.iter()
        .flat_map(|(name, src)| {
            let name_clone = name.clone();
            let src_clone = src.clone();
            
            WalkDir::new(&src_clone)
                .parallelism(Parallelism::RayonDefaultPool { busy_timeout: Duration::new(5, 0) })
                .into_iter()
                .filter_map(move |entry| {
                    let entry = entry.ok()?;
                    if !entry.file_type().is_file() { return None; }
                    
                    let rel = entry.path().strip_prefix(&src_clone).unwrap().to_path_buf();
                    Some((name_clone.clone(), entry.path().to_path_buf(), rel))
                })
        })
        .collect();

    scan_pb.finish_and_clear();
    eprintln!("📊 Found {} files across {} sources.\n", all_paths.len(), sources.len());

    // ─────────────────────────────────────────────────────────────
    // Phase 2: Parallel Hashing (Deterministic Progress)
    // ─────────────────────────────────────────────────────────────
    let hash_pb = ProgressBar::new(all_paths.len() as u64);
    hash_pb.set_style(
        ProgressStyle::with_template("{spinner:.blue} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} {msg}")
            .unwrap()
    );
    hash_pb.set_message("⚡ Hashing with BLAKE3...");

    let all_entries: Vec<FileEntry> = all_paths
        .par_iter()
        .filter_map(|(name, abs, rel)| {
            match hash_file(abs) {
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

    // ─────────────────────────────────────────────────────────────
    // Phase 3: Group & Parallel Copy/Reflink
    // ─────────────────────────────────────────────────────────────
    let mut db: HashMap<PathBuf, Vec<FileEntry>> = HashMap::new();
    for entry in all_entries {
        db.entry(entry.rel_path.clone()).or_default().push(entry);
    }

    let copy_pb = ProgressBar::new(db.len() as u64);
    copy_pb.set_style(
        ProgressStyle::with_template("{spinner:.blue} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} {msg}")
            .unwrap()
    );
    copy_pb.set_message("📦 Copying & deduplicating...");

    db.par_iter().for_each(|(_rel_path, entries)| {
        let unique_hashes: HashSet<[u8; 32]> = entries.iter().map(|e| e.hash).collect();
        
        if unique_hashes.len() == 1 {
            let _ = copy_file(&entries[0], &args.consolidated, args.dry_run)
                .map_err(|e| eprintln!("⚠️  Consolidate failed: {}", e));
        } else {
            for entry in entries {
                let dst_base = args.collision.join(&entry.source_name);
                let _ = copy_file(entry, &dst_base, args.dry_run)
                    .map_err(|e| eprintln!("⚠️  Collision copy failed: {}", e));
            }
        }
        copy_pb.inc(1);
    });

    copy_pb.finish_and_clear();
    
    // Final summary counts
    let cons_count = db.iter().filter(|(_, v)| v.iter().map(|e| e.hash).collect::<HashSet<_>>().len() == 1).count();
    let coll_count = db.iter().filter(|(_, v)| v.iter().map(|e| e.hash).collect::<HashSet<_>>().len() > 1)
                       .map(|(_, v)| v.len()).sum::<usize>();

    eprintln!("\n🏁 Done!");
    eprintln!("  📂 Consolidated: {} unique paths", cons_count);
    eprintln!("  💥 Collisions:   {} files", coll_count);
    Ok(())
}
