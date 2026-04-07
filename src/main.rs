mod args;
mod debug;
mod fs;
mod hasher;
mod progress;

use ahash::AHashMap;
use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use jwalk::{Parallelism, WalkDir};
use rayon::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use args::{Args, SourceSpec};
use fs::{copy_file, create_subvol_or_dir, FileEntry};
use hasher::hash_file_cached;
use progress::{ckpt_path, ProgressState};

fn main() -> Result<()> {
    let args = Args::parse();

    // Validate arguments and parse sources
    let sources: Vec<SourceSpec> = args.validate()?;

    // Always configure the Rayon thread pool explicitly so we can set the
    // worker-thread stack size.
    // In particular, `blake3::Hasher::update_rayon` splits large
    // files via recursive rayon::join calls that all execute on the *same*
    // worker-thread stack; the default 8 MB stack overflows for files in the
    // gigabyte range `(recursion depth ≈ log₂(file_size / chunk_threshold))`.
    // 32 MB gives comfortable headroom for any realistic file size.
    // If you are on a memory constrainted system... Well, it sucks to be you.
    // Sorry.
    {
        let mut builder = rayon::ThreadPoolBuilder::new().stack_size(32 * 1024 * 1024);
        if let Some(n) = args.threads {
            builder = builder.num_threads(n);
        }
        builder
            .build_global()
            .context("Failed to configure Rayon thread pool")?;
    }

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
                if !args.is_quiet() {
                    eprintln!(
                        "🔄 Resuming: {} previously completed paths, {} cached hashes.\n",
                        s.completed.len(),
                        s.hash_cache.len()
                    );
                }
            } else {
                if !args.is_quiet() {
                    eprintln!(
                        "⚠️  --resume specified but no progress file found at {:?}. Starting fresh.\n",
                        progress_file
                    );
                }
            }
        } else {
            // Fresh run: keep hash cache for speed but reset completion records
            s.completed.clear();
        }
        Arc::new(RwLock::new(s))
    };

    if !args.dry_run {
        create_subvol_or_dir(&args.consolidated).context("Failed to create consolidated dir")?;
        create_subvol_or_dir(&args.collision).context("Failed to create collision dir")?;
    }

    if args.debug {
        // Convert SourceSpec to the format expected by debug module
        let debug_sources: Vec<(String, PathBuf)> = sources
            .iter()
            .map(|s| (s.name.clone(), s.path.clone()))
            .collect();
        debug::print_debug_info(&args, &debug_sources, &progress_file);
    }

    // ─────────────────────────────────────────────────────────────
    // Phase 1: Parallel Scan
    // ─────────────────────────────────────────────────────────────
    let scan_pb = ProgressBar::new_spinner();
    scan_pb.enable_steady_tick(Duration::from_millis(100));
    scan_pb.set_style(ProgressStyle::with_template("{spinner:.green} {msg}").unwrap());
    scan_pb.set_message("🔍 Scanning filesystem in parallel...");

    // Use Arc<str> for source names — Arc::clone is a cheap atomic increment
    // rather than a heap allocation, saving ~1 alloc per file per source.
    // Use Arc<PathBuf> for rel paths so the same allocation is shared between
    // all_paths, FileEntry, and the db HashMap key (no PathBuf clone per entry).
    let is_quiet = args.is_quiet();
    let all_paths: Vec<(Arc<str>, PathBuf, Arc<PathBuf>)> = sources
        .iter()
        .flat_map(|source| {
            let name_arc: Arc<str> = Arc::from(source.name.as_str());
            let src_clone = source.path.clone();
            WalkDir::new(&src_clone)
                .parallelism(Parallelism::RayonDefaultPool {
                    busy_timeout: Duration::new(5, 0),
                })
                .into_iter()
                .filter_map(move |entry| {
                    let entry = entry.ok()?;
                    let ft = entry.file_type();
                    if ft.is_symlink() {
                        if !is_quiet {
                            eprintln!("⚠️  Skipping symlink: {}", entry.path().display());
                        }
                        return None;
                    }
                    if !ft.is_file() {
                        return None;
                    }
                    let rel =
                        Arc::new(entry.path().strip_prefix(&src_clone).unwrap().to_path_buf());
                    Some((Arc::clone(&name_arc), entry.path().to_path_buf(), rel))
                })
        })
        .collect();

    scan_pb.finish_and_clear();
    if !args.is_quiet() {
        eprintln!(
            "📊 Found {} files across {} sources.\n",
            all_paths.len(),
            sources.len()
        );
    }

    // ─────────────────────────────────────────────────────────────
    // Phase 2: Parallel Hashing (with cache + resume skip)
    // ─────────────────────────────────────────────────────────────
    let hash_pb = ProgressBar::new(all_paths.len() as u64);
    hash_pb.enable_steady_tick(Duration::from_millis(100));
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
            // In resume mode, skip files whose rel_path was already completed.
            // RwLock read — multiple threads can check simultaneously.
            if args.resume {
                let rel_str = rel.to_string_lossy().to_string();
                if state.read().unwrap().completed.contains(&rel_str) {
                    hash_pb.inc(1);
                    return None;
                }
            }

            match hash_file_cached(abs, &state) {
                Ok(hash) => {
                    hash_pb.inc(1);
                    Some(FileEntry {
                        source_name: Arc::clone(name),
                        abs_path: abs.clone(),
                        rel_path: Arc::clone(rel),
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

    // Persist hash cache after hashing phase.
    // read() is sufficient — save() takes &self.
    if !args.dry_run {
        if let Err(e) = state.read().unwrap().save(&progress_file) {
            eprintln!("⚠️  Failed to save hash cache: {}", e);
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Phase 3: Group & Parallel Copy/Reflink
    // ─────────────────────────────────────────────────────────────
    // Arc::clone on rel_path shares the existing PathBuf allocation as the
    // HashMap key — no extra heap alloc per entry.
    let mut db: AHashMap<Arc<PathBuf>, Vec<FileEntry>> = AHashMap::with_capacity(all_entries.len());
    for entry in all_entries {
        let key = Arc::clone(&entry.rel_path);
        db.entry(key).or_default().push(entry);
    }

    let copy_pb = ProgressBar::new(db.len() as u64);
    copy_pb.enable_steady_tick(Duration::from_millis(100));
    copy_pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.blue} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} {msg}",
        )
        .unwrap(),
    );
    copy_pb.set_message("📦 Copying & deduplicating...");

    // Open the append-only checkpoint log before starting the copy phase.
    // Each successful completion appends a single line (~60 bytes).
    // No more serialising the entire hash cache on every N-th file.
    let ckpt_writer: Option<Arc<std::sync::Mutex<std::fs::File>>> = if !args.dry_run {
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(ckpt_path(&progress_file))
            .context("Failed to open checkpoint log")?;
        Some(Arc::new(std::sync::Mutex::new(f)))
    } else {
        None
    };

    // Atomic counters — updated without a lock on every file.
    let cons_count = AtomicUsize::new(0);
    let coll_count = AtomicUsize::new(0);

    db.par_iter().for_each(|(rel_path, entries)| {
        let rel_str = rel_path.to_string_lossy().to_string();

        // Allocation-free uniqueness check: compare every hash to the first.
        // Replaces the previous HashSet<[u8; 32]> creation per group.
        let first_hash = entries[0].hash;
        let all_same = entries.iter().all(|e| e.hash == first_hash);

        let success = if all_same {
            let dst = args.consolidated.join(rel_path.as_ref());
            if dst.exists() {
                // Destination already in Consolidated — check content.
                match hash_file_cached(&dst, &state) {
                    Ok(existing_hash) if existing_hash == first_hash => {
                        // Identical content already in place — nothing to do.
                        if args.debug {
                            copy_pb.println(format!(
                                "[SKIP    ] {} (identical content already in Consolidated)",
                                rel_path.display()
                            ));
                        }
                        true
                    }
                    Ok(_) => {
                        // Different content — conflict; route to Collision.
                        eprintln!(
                            "⚠️  Conflict: {} already exists with different content. Routing to Collision.",
                            dst.display()
                        );
                        entries.iter().all(|entry| {
                            let dst_base = args.collision.join(entry.source_name.as_ref());
                            copy_file(entry, &dst_base, args.dry_run, args.debug, &copy_pb)
                                .map_err(|e| eprintln!("⚠️  Collision copy failed: {}", e))
                                .is_ok()
                        })
                    }
                    Err(e) => {
                        eprintln!(
                            "⚠️  Could not hash existing consolidated file {}: {}. Skipping.",
                            dst.display(),
                            e
                        );
                        false
                    }
                }
            } else {
                copy_file(&entries[0], &args.consolidated, args.dry_run, args.debug, &copy_pb)
                    .map_err(|e| eprintln!("⚠️  Consolidate failed: {}", e))
                    .is_ok()
            }
        } else {
            entries.iter().all(|entry| {
                let dst_base = args.collision.join(entry.source_name.as_ref());
                let collision_dst = dst_base.join(entry.rel_path.as_ref());
                if collision_dst.exists() {
                    if args.debug {
                        copy_pb.println(format!(
                            "[SKIP    ] {} (already in Collision)",
                            collision_dst.display()
                        ));
                    }
                    return true;
                }

                copy_file(entry, &dst_base, args.dry_run, args.debug, &copy_pb)
                    .map_err(|e| eprintln!("⚠️  Collision copy failed: {}", e))
                    .is_ok()
            })
        };

        enum Outcome {
            Consolidated,
            Collision(usize),  // number of files routed to collision
            Skipped,
        }

        let outcome = if all_same {
            let dst = args.consolidated.join(rel_path.as_ref());
            if dst.exists() {
                match hash_file_cached(&dst, &state) {
                    Ok(existing_hash) if existing_hash == first_hash => Outcome::Consolidated,
                    Ok(_) => Outcome::Collision(entries.len()),  // Conflict with existing file
                    Err(_) => Outcome::Skipped,
                }
            } else {
                Outcome::Consolidated
            }
        } else {
            Outcome::Collision(entries.len())  // Hash mismatch among sources
        };

        // Update counters based on actual outcome
        match outcome {
            Outcome::Consolidated => cons_count.fetch_add(1, Ordering::Relaxed),
            Outcome::Collision(n) => coll_count.fetch_add(n, Ordering::Relaxed),
            Outcome::Skipped => 0
        };

        if success && !args.dry_run {
            // Update in-memory completed set (brief write lock).
            state.write().unwrap().completed.insert(rel_str.clone());

            // Append to the checkpoint log — one tiny write, no full serialisation.
            if let Some(ref writer) = ckpt_writer {
                use std::io::Write;
                let _ = writeln!(writer.lock().unwrap(), "{}", rel_str);
            }
        }

        copy_pb.inc(1);
    });

    copy_pb.finish_and_clear();

    // Close the checkpoint log before the final full save.
    drop(ckpt_writer);

    // Final full save — persists hash cache + completed set.
    // Deletes the checkpoint log on success (it is now redundant).
    if !args.dry_run {
        if let Err(e) = state.read().unwrap().save(&progress_file) {
            eprintln!("⚠️  Failed to save final progress: {}", e);
        } else {
            let _ = std::fs::remove_file(ckpt_path(&progress_file));
        }
    }

    // Summary
    let cons = cons_count.load(Ordering::Relaxed);
    let coll = coll_count.load(Ordering::Relaxed);
    let total_completed = state.read().unwrap().completed.len();

    eprintln!("\n🏁 Done!");
    eprintln!("  📂 Consolidated: {} unique paths", cons);
    eprintln!("  💥 Collisions:   {} files", coll);
    if args.resume && total_completed > cons + coll {
        eprintln!(
            "  ⏭️  Skipped (already done): {} paths",
            total_completed.saturating_sub(cons + coll)
        );
    }
    eprintln!("  📊 Total completed: {} paths", total_completed);
    Ok(())
}
