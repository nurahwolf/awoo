# AWOO (Better Name Pending)

The intent of this program / script is to 'merge' many subvolumes into one big homogeneous subvolume.

THIS PROGRAM DOES **NOT** MODIFY THE SOURCE DATA FED INTO IT IN ANY WAY! Cleanup of those is manual, by design.

It checks file hashes and if any collisions are found then they are moved to a collision folder.

Files are moved to the exact same path as their source, so:

```
Storage_A:
  - Hi
  - Hello/Hi
Storage_B:
  - Hello/Hello
Storage_C:
  - Hello/Hi
  - Jeff/Jeff
```
Would result in:

```
Consolidated:
 - Hi
 - Hello/Hi
 - Hello/Hello
 - Jeff/Jeff
Collision:
 - Storage_C:
   - Hello/Hi
```

I would recommend running the awesome [Best-Effort Extent-Same](https://github.com/Zygo/bees) project first, to deduplicate file blocks.
Once you are done and any collisions manually dealt with, you can then delete the original subvolumes.

While this project is *designed* to work with BTRFS subvolumes, it is coded in a way that it should be usable with any linux filesystem.

## How To Run

```shell
# 1. Build (requires Rust toolchain)
cargo build --release

# 2. Dry run first (highly recommended)
./target/release/awoo \
  "Storage_A:/mnt/btrfs/A" \
  "Storage_B:/mnt/btrfs/B" \
  "Storage_C:/mnt/btrfs/C" \
  --dry-run

# 3. Execute
./target/release/awoo \
  "Storage_A:/mnt/btrfs/A" \
  "Storage_B:/mnt/btrfs/B" \
  "Storage_C:/mnt/btrfs/C" \
  -o ./Consolidated -c ./Collision
```

## Limiting I/O
Hashing speed will be limited by your I/O capability, in particular read throughput. Run during off-peak hours might be smart, or use `ionice -c 2 -n 7 ./target/release/awoo ...` to limit IO priority.

## Resume Support

If a run is interrupted (power loss, Ctrl-C, full disk, etc.) you can pick up where you left off instead of starting over.

A progress file is automatically written to `<consolidated>/.awoo_progress.json` (override with `--progress-file`). It stores two things:

- **Completed paths** — relative paths that were fully and successfully copied this run.
- **Hash cache** — a record of each file's `mtime`, `size`, and BLAKE3 hash so that unchanged files are never re-hashed on a subsequent run, even without `--resume`.

### Resuming an interrupted run

Pass `--resume` and use the exact same arguments as the original run:

```shell
./target/release/awoo \
  "Storage_A:/mnt/btrfs/A" \
  "Storage_B:/mnt/btrfs/B" \
  "Storage_C:/mnt/btrfs/C" \
  -o ./Consolidated -c ./Collision \
  --resume
```

awoo will skip every path already recorded in the progress file and only process the remainder. A summary at the end shows how many paths were skipped.

### Custom progress file location

```shell
./target/release/awoo \
  "Storage_A:/mnt/btrfs/A" \
  "Storage_B:/mnt/btrfs/B" \
  "Storage_C:/mnt/btrfs/C" \
  -o ./Consolidated -c ./Collision \
  --progress-file /tmp/my_merge.json \
  --resume
```

### Starting fresh (while keeping the hash cache)

Omitting `--resume` starts a clean run, as in all files are re-processed, though the hash cache from previous runs is still used to skip redundant BLAKE3 I/O for unchanged files.

```shell
# No --resume: completed set is cleared, hash cache is still used for speed
./target/release/awoo \
  "Storage_A:/mnt/btrfs/A" \
  "Storage_B:/mnt/btrfs/B" \
  "Storage_C:/mnt/btrfs/C" \
  -o ./Consolidated -c ./Collision
```
