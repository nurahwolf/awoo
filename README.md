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

I would recommend running the awesome (Best-Effort Extent-Same)[https://github.com/Zygo/bees] project first, to deduplicate file blocks.
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
