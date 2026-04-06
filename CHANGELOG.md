# Changelog

All notable changes to this project will be documented in this file.

## [0.0.5] - Performance Optimisations

### Added
- `--threads N` flag to override the Rayon thread pool size. Over-subscribing (e.g. 2× CPU count) can improve throughput on I/O-heavy hardware by keeping storage saturated while some threads wait.
- BLAKE3 large-file parallelism: files ≥ 256 MB are memory-mapped and hashed using `blake3::Hasher::update_rayon`, distributing the work across all available threads for multi-core throughput on individual files.
- `--debug` copy phase now prints a per-file label showing which copy method was used (`[FICLONE ]`, `[O_DIRECT]`, `[CP      ]`, `[SKIP    ]`) rendered cleanly above the active progress bar via `indicatif`'s `println`.

### Changed
- **Concurrency**: `Arc<Mutex<ProgressState>>` replaced with `Arc<RwLock<ProgressState>>`. Hash-cache lookups (read-heavy in Phase 2) now run concurrently across all Rayon threads; only cache-miss writes take an exclusive lock.
- **Hash cache format**: `HashCacheEntry.hash` is now stored as raw `[u8; 32]` bytes in memory instead of a 64-character hex `String`, eliminating a hex-decode on every cache hit and halving per-entry memory. The on-disk JSON format is unchanged.
- **Faster hash maps**: `std::collections::HashMap`/`HashSet` replaced throughout with `AHashMap`/`AHashSet` from the `ahash` crate (3–5× faster non-cryptographic hashing for internal data structures).
- **Allocation-free deduplication check**: the per-group `HashSet<[u8; 32]>` creation in Phase 3 replaced with a branchless `Iterator::all` comparison against the first hash — zero allocations per file group.
- **Zero-copy `db` construction**: `FileEntry.rel_path` and the Phase 3 `db` HashMap key now share a single `Arc<PathBuf>` allocation. Building the map uses `Arc::clone` (an atomic increment) instead of a `PathBuf` heap allocation per entry.
- **`Arc<str>` source names**: source name strings are converted to `Arc<str>` once per source and reference-counted into every `FileEntry`, replacing one `String` heap allocation per file.
- **Thread-local `create_dir_all` cache**: each Rayon worker maintains a thread-local set of directories it has already created, short-circuiting redundant `stat` syscalls for files sharing a parent directory.
- **Append-only checkpoint log**: Phase 3 checkpoints now append a single line (~60 bytes) per completion to a `.ckpt` file instead of rewriting the entire completed set as JSON. For a 1.4 M file dataset this reduces checkpoint write amplification from ~118 GB down to ~84 MB total. The old JSON checkpoint format is still read on resume for backward compatibility.
- **Atomic summary counters**: `cons_count` and `coll_count` are tracked with `AtomicUsize` increments during the copy phase, replacing the end-of-run double-scan of the `db` map.
- **Progress bar `steady_tick`**: all three progress bars now redraw at a fixed 10 fps via `enable_steady_tick(100 ms)`, decoupling terminal I/O from file throughput and reducing `indicatif` lock contention at high file counts.

### Fixed
- `blake3` crate now explicitly enables the `rayon` feature flag, ensuring `update_rayon` is always available regardless of what other crates happen to be present in the dependency graph.

## [0.0.4] - Overwrite Protection & Code Refactor

### Fixed
- Copying a new source into an already-populated `Consolidated` directory no longer overwrites existing files.
  - If the destination exists with **identical** content (same BLAKE3 hash), the file is silently skipped.
  - If the destination exists with **different** content, the incoming file is routed to `Collision` and a warning is emitted, leaving the existing consolidated file untouched.
- Files already present in `Collision` from a previous run are now skipped rather than overwritten.

### Changed
- `src/main.rs` has been split into four focused modules for maintainability:
  - `args.rs` — CLI argument definition (`Args`)
  - `progress.rs` — progress state, hash cache, and serialisation (`ProgressState`, `HashCacheEntry`, `SAVE_INTERVAL`)
  - `hasher.rs` — BLAKE3 hashing with cache (`hash_file`, `hash_file_cached`)
  - `fs.rs` — all filesystem operations (`FileEntry`, `copy_file`, `create_subvol_or_dir`, reflink logic)

## [0.0.3] - BTRFS Subvolume Output

### Added
- Output directories (`Consolidated` and `Collision`) are now created as BTRFS subvolumes when running on a BTRFS filesystem, using the native `BTRFS_IOC_SUBVOL_CREATE` ioctl.
- Graceful fallback to standard directory creation if the ioctl fails (e.g. not on a BTRFS volume).
- No action is taken if the output paths already exist.

## [0.0.2] - Resume Support

### Added
- `--resume` flag to skip already-processed files from a previous interrupted run.
- `--progress-file` flag to override the default progress file location (`<consolidated>/.awoo_progress.json`).
- Hash cache persisted to the progress file, unchanged files (matched by `mtime` + `size`) are never re-hashed, even without `--resume`.
- Progress is checkpointed every 50 completed paths and once more at the end of a run.

## [0.0.1] - Initial Release

### Added
- Parallel filesystem scan across multiple named source directories using `jwalk`.
- BLAKE3 file hashing with a 256 KB read buffer via `rayon` thread pool (Can this be improved / is it sound?).
- File deduplication by relative path and hash, with unique files going to `Consolidated` and conflicts go to `Collision`.
- Native BTRFS reflink support via `FICLONE` ioctl with automatic fallback to `std::fs::copy`.
- `--dry-run` flag to preview operations without writing any files (Otherwise it will explode).
- Progress bars for all three phases (scan, hash, copy) via `indicatif`.
