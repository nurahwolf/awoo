use anyhow::{Context, Result};
#[cfg(target_os = "linux")]
use std::fs::File;
use std::path::{Path, PathBuf};

pub struct FileEntry {
    pub source_name: String,
    pub abs_path: PathBuf,
    pub rel_path: PathBuf,
    pub hash: [u8; 32],
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

/// Copy file using Btrfs reflink if available (native ioctl), with fallback
pub fn copy_file(entry: &FileEntry, dst_base: &Path, dry_run: bool) -> Result<()> {
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
