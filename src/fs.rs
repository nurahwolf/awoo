use anyhow::{Context, Result};
use indicatif::ProgressBar;
#[cfg(target_os = "linux")]
use std::fs::File;
use std::path::{Path, PathBuf};

pub struct FileEntry {
    pub source_name: String,
    pub abs_path: PathBuf,
    pub rel_path: PathBuf,
    pub hash: [u8; 32],
}

/// Copy file using native Btrfs FICLONE ioctl (Linux only).
/// Returns an error if the ioctl fails for any reason.
#[cfg(target_os = "linux")]
fn reflink_file(src: &Path, dst: &Path) -> Result<()> {
    use std::fs::OpenOptions;
    use std::os::unix::io::AsRawFd;

    // FICLONE = _IOW(0x94, 9, int) = 0x40049409
    // Stable Linux UAPI constant: https://github.com/torvalds/linux/blob/master/include/uapi/linux/fs.h#L335
    const FICLONE: libc::c_ulong = 0x40049409;

    let src_file = File::open(src).with_context(|| format!("Failed to open source {:?}", src))?;

    // Destination must be a newly created, empty file for FICLONE to work.
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

/// A 256 KB buffer with the 4096-byte alignment required by `O_DIRECT`.
///
/// `repr(C, align(4096))` guarantees the alignment. The `Box` used at the
/// call site keeps the 256 KB off the stack; in release mode rustc allocates
/// the value directly on the heap without a stack intermediate.
#[cfg(target_os = "linux")]
#[repr(C, align(4096))]
struct DirectBuf([u8; 256 * 1024]);

/// Copy file content from `src` to `tmp`, including Unix permission bits.
///
/// On Linux, opens `src` with `O_DIRECT` to read without polluting the page
/// cache, writes to `tmp` with ordinary buffered I/O, then calls
/// `POSIX_FADV_DONTNEED` on `tmp` to evict the freshly written pages — we
/// stored those bytes for long-term use and won't be re-reading them soon.
/// Falls back to `std::fs::copy` (which uses `copy_file_range` on Linux) if
/// the filesystem does not support `O_DIRECT`.
fn copy_content(src: &Path, tmp: &Path) -> Result<bool> {
    #[cfg(target_os = "linux")]
    {
        use std::io::{Read, Write};
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::io::AsRawFd;

        let direct_src = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(src);

        if let Ok(mut src_file) = direct_src {
            let mut tmp_file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(tmp)
                .with_context(|| format!("Failed to create {:?}", tmp))?;

            let mut buf = Box::new(DirectBuf([0u8; 256 * 1024]));
            loop {
                let n = src_file
                    .read(&mut buf.0)
                    .context("Failed to read source file (O_DIRECT)")?;
                if n == 0 {
                    break;
                }
                tmp_file
                    .write_all(&buf.0[..n])
                    .context("Failed to write destination file")?;
            }

            // Evict the destination pages — we wrote this file for long-term
            // storage and will not read it again in this run.
            // SAFETY: fd is valid; POSIX_FADV_DONTNEED is a well-defined constant.
            unsafe {
                libc::posix_fadvise(tmp_file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
            }

            // Copy Unix permission bits (std::fs::copy does this automatically;
            // our manual loop must do it explicitly).
            if let Ok(meta) = std::fs::metadata(src) {
                let _ = tmp_file.set_permissions(meta.permissions());
            }

            return Ok(true); // O_DIRECT was used
        }
        // O_DIRECT not supported on this filesystem — fall through.
    }

    // Non-Linux or O_DIRECT unavailable: std::fs::copy handles content + permissions.
    std::fs::copy(src, tmp).with_context(|| format!("Failed to copy {:?} to {:?}", src, tmp))?;
    Ok(false) // std::fs::copy was used
}

/// Copy all non-content metadata from `src` to `dst`: extended attributes,
/// ownership (uid/gid), and timestamps.
///
/// All steps are best-effort — errors are silently ignored for filesystems or
/// permission levels that do not support a given feature.
fn copy_metadata(src: &Path, dst: &Path) -> Result<()> {
    // Extended attributes — covers user xattrs, POSIX ACLs, SELinux labels, etc.
    #[cfg(unix)]
    if let Ok(names) = xattr::list(src) {
        for name in names {
            if let Ok(Some(value)) = xattr::get(src, &name) {
                let _ = xattr::set(dst, &name, &value);
            }
        }
    }

    // Ownership (uid/gid) — best-effort; requires CAP_CHOWN or root.
    // Must come before timestamps: chown resets atime on some kernels.
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let Ok(meta) = std::fs::metadata(src) {
            let _ = std::os::unix::fs::chown(dst, Some(meta.uid()), Some(meta.gid()));
        }
    }

    // Timestamps — last, so chown above cannot reset them.
    if let Ok(meta) = std::fs::metadata(src) {
        let atime = filetime::FileTime::from_last_access_time(&meta);
        let mtime = filetime::FileTime::from_last_modification_time(&meta);
        let _ = filetime::set_file_times(dst, atime, mtime);
    }

    Ok(())
}

/// Cross-platform wrapper: tries a Btrfs reflink (Linux) first, then an atomic
/// Direct I/O copy, then renames into place.
///
/// When `debug` is true, prints a one-line label for each file showing which
/// copy method was actually used. Output is routed through `pb.println` so it
/// renders cleanly above the active progress bar rather than corrupting it.
///
/// - FICLONE path: already atomic and preserves all metadata natively.
/// - Fallback path: `copy_content` (O_DIRECT read + cache eviction) followed
///   by `copy_metadata` (xattrs, ownership, timestamps), then an atomic rename.
///   A crash or disk-full mid-write leaves `dst` untouched.
fn copy_with_reflink_fallback(src: &Path, dst: &Path, debug: bool, pb: &ProgressBar) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        // FICLONE is already atomic and preserves all metadata — nothing else needed.
        if reflink_file(src, dst).is_ok() {
            if debug {
                pb.println(format!("[FICLONE ] {}", dst.display()));
            }
            return Ok(());
        }
    }

    // Write to a hidden temp file in the same directory so the rename is guaranteed
    // to be on the same filesystem (POSIX rename is atomic within one filesystem).
    let tmp = dst.with_file_name(format!(
        ".{}.awoo_tmp",
        dst.file_name().unwrap_or_default().to_string_lossy()
    ));

    let method: Result<bool> = (|| -> Result<bool> {
        let used_direct = copy_content(src, &tmp)?;
        copy_metadata(src, &tmp)?;
        std::fs::rename(&tmp, dst)
            .with_context(|| format!("Failed to rename {:?} to {:?}", &tmp, dst))?;
        Ok(used_direct)
    })();

    match method {
        Ok(used_direct) => {
            if debug {
                if used_direct {
                    pb.println(format!("[O_DIRECT] {}", dst.display()));
                } else {
                    pb.println(format!("[CP      ] {}", dst.display()));
                }
            }
            Ok(())
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp); // best-effort cleanup on any failure
            Err(e)
        }
    }
}

/// Creates `path` as a BTRFS subvolume if possible, otherwise falls back to a regular directory.
/// Does nothing if `path` already exists.
#[cfg(target_os = "linux")]
pub fn create_subvol_or_dir(path: &Path) -> Result<()> {
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
pub fn create_subvol_or_dir(path: &Path) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    std::fs::create_dir_all(path)
        .with_context(|| format!("Failed to create directory {:?}", path))?;
    Ok(())
}

/// Copy a `FileEntry` to `dst_base/entry.rel_path`, using a Btrfs reflink where possible.
///
/// All terminal output (dry-run notices and debug copy-method labels) is routed
/// through `pb.println` so it prints cleanly above the active progress bar.
pub fn copy_file(
    entry: &FileEntry,
    dst_base: &Path,
    dry_run: bool,
    debug: bool,
    pb: &ProgressBar,
) -> Result<()> {
    let dst = dst_base.join(&entry.rel_path);

    if dry_run {
        pb.println(format!(
            "[DRY    ] {} -> {}",
            entry.abs_path.display(),
            dst.display()
        ));
        return Ok(());
    }

    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory {:?}", parent))?;
    }

    copy_with_reflink_fallback(&entry.abs_path, &dst, debug, pb)?;
    Ok(())
}
