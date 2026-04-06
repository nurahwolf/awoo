# Changelog

All notable changes to this project will be documented in this file.

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
