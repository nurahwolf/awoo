mod args;
mod fs;
mod hasher;
mod progress;

use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use jwalk::{Parallelism, WalkDir};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use args::Args;
use fs::{copy_file, create_subvol_or_dir, FileEntry};
use hasher::hash_file_cached;
use progress::{ProgressState, SAVE_INTERVAL};

/// Returns a best-effort absolute path. Tries `canonicalize` first (resolves symlinks);
/// falls back to prepending the current working directory for paths that don't exist yet.
fn resolve_path(path: &std::path::Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(path)
        }
    })
}

/// Returns true if either path is a prefix of the other (i.e. one contains the other).
/// Uses `Path::starts_with` which compares whole components, not raw string prefixes.
fn paths_overlap(a: &std::path::Path, b: &std::path::Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
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

    // ── Startup validation ────────────────────────────────────────
    // 1. Duplicate source names — collision files are bucketed by name,
    //    so two sources sharing a name would silently overwrite each other.
    {
        let mut seen: HashSet<&str> = HashSet::new();
        for (name, _) in &sources {
            if !seen.insert(name.as_str()) {
                anyhow::bail!(
                    "Duplicate source name '{name}'. Each source must have a unique label."
                );
            }
        }
    }

    // 2. Source / output overlap — scanning a path that contains (or is contained
    //    by) an output directory corrupts the run.
    {
        let cons_abs = resolve_path(&args.consolidated);
        let coll_abs = resolve_path(&args.collision);

        if paths_overlap(&cons_abs, &coll_abs) {
            anyhow::bail!(
                "Consolidated ({cons_abs:?}) and Collision ({coll_abs:?}) directories overlap."
            );
        }

        for (name, src) in &sources {
            if paths_overlap(src, &cons_abs) {
                anyhow::bail!(
                    "Source '{name}' ({src:?}) overlaps with the Consolidated output directory ({cons_abs:?})."
                );
            }
            if paths_overlap(src, &coll_abs) {
                anyhow::bail!(
                    "Source '{name}' ({src:?}) overlaps with the Collision output directory ({coll_abs:?})."
                );
            }
        }
    }
    // ─────────────────────────────────────────────────────────────

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
                    let ft = entry.file_type();
                    if ft.is_symlink() {
                        eprintln!("⚠️  Skipping symlink: {}", entry.path().display());
                        return None;
                    }
                    if !ft.is_file() {
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
            let dst = args.consolidated.join(rel_path);
            if dst.exists() {
                // Destination already in Consolidated — check whether it is the same content
                match hash_file_cached(&dst, &state) {
                    Ok(existing_hash) if existing_hash == entries[0].hash => {
                        // Identical content already in place — nothing to do
                        true
                    }
                    Ok(_) => {
                        // Different content — conflict with an existing consolidated file;
                        // route the incoming file(s) to Collision and leave the existing one alone
                        eprintln!(
                            "⚠️  Conflict: {} already exists with different content. Routing to Collision.",
                            dst.display()
                        );
                        entries.iter().all(|entry| {
                            let dst_base = args.collision.join(&entry.source_name);
                            copy_file(entry, &dst_base, args.dry_run)
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
                copy_file(&entries[0], &args.consolidated, args.dry_run)
                    .map_err(|e| eprintln!("⚠️  Consolidate failed: {}", e))
                    .is_ok()
            }
        } else {
            entries.iter().all(|entry| {
                let dst_base = args.collision.join(&entry.source_name);
                let collision_dst = dst_base.join(&entry.rel_path);
                if collision_dst.exists() {
                    // Already written to Collision in a previous run — skip
                    return true;
                }
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
