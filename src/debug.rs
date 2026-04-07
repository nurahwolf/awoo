use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::args::Args;

/// Print a comprehensive diagnostic summary covering platform, configuration,
/// filesystem types, probed I/O capabilities, and the resulting copy strategy.
///
/// Called from `main` when `--debug` is passed, after output directories have
/// been created (so probing can target the actual destination filesystem).
pub fn print_debug_info(args: &Args, sources: &[(String, PathBuf)], progress_file: &Path) {
    let sep = "═".repeat(66);
    eprintln!("\n🐛 Debug Information");
    eprintln!("{sep}");

    // ── Platform ──────────────────────────────────────────────────────────
    eprintln!("\nPlatform");
    eprintln!(
        "  OS / arch:     {} / {}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    #[cfg(target_os = "linux")]
    eprintln!("  Kernel:        {}", kernel_version());

    // ── Configuration ────────────────────────────────────────────────────
    eprintln!("\nConfiguration");
    eprintln!("  Dry-run:       {}", args.dry_run);
    eprintln!("  Resume:        {}", args.resume);
    eprintln!("  Progress file: {}", progress_file.display());
    eprintln!("  Consolidated:  {}", args.output.display());
    eprintln!("  Collision:     {}", args.collision.display());
    eprintln!("  Sources ({}):", sources.len());
    for (name, path) in sources {
        eprintln!("    {name:<16}  →  {}", path.display());
    }

    // ── Filesystems ──────────────────────────────────────────────────────
    eprintln!("\nFilesystems");
    let mut seen: HashSet<PathBuf> = HashSet::new();
    for (name, path) in sources {
        if seen.insert(path.clone()) {
            eprintln!("  {name:<18} →  {}", fs_type(path));
        }
    }
    if args.output.exists() {
        eprintln!("  {:<18} →  {}", "Consolidated", fs_type(&args.output));
    }
    if args.collision.exists() {
        eprintln!("  {:<18} →  {}", "Collision", fs_type(&args.collision));
    }

    // ── I/O Capability Probes ────────────────────────────────────────────
    // Probe on the destination filesystem where possible; fall back to CWD.
    let probe_dir: PathBuf = if args.output.exists() {
        args.output.clone()
    } else {
        PathBuf::from(".")
    };
    eprintln!(
        "\nI/O Capabilities            (probed on: {})",
        probe_dir.display()
    );

    // On Linux we actively probe both capabilities.
    // On other platforms both are reported as unsupported.
    #[cfg(target_os = "linux")]
    let (reflink_ok, odirect_ok): (Option<bool>, Option<bool>) = {
        // Skip the reflink write-probe in dry-run when the output dir doesn't exist yet.
        let r = if args.dry_run && !args.consolidated.exists() {
            None
        } else {
            probe_reflink(&probe_dir)
        };
        let d = probe_direct_io(&probe_dir);
        (r, d)
    };

    #[cfg(not(target_os = "linux"))]
    let (reflink_ok, odirect_ok): (Option<bool>, Option<bool>) = (Some(false), Some(false));

    let fmt_cap = |v: Option<bool>| match v {
        Some(true) => "✅  supported",
        Some(false) => "❌  not supported",
        None => "⚠️   not probed (dry-run, output dir not yet created)",
    };

    eprintln!("  Btrfs reflink (FICLONE):  {}", fmt_cap(reflink_ok));
    eprintln!("  Direct I/O (O_DIRECT):    {}", fmt_cap(odirect_ok));

    // ── Copy Strategy ────────────────────────────────────────────────────
    eprintln!("\nCopy Strategy");

    #[cfg(target_os = "linux")]
    {
        let hash_reads = if odirect_ok == Some(true) {
            "O_DIRECT  (bypasses page cache)"
        } else {
            "buffered  (std::fs — O_DIRECT unavailable)"
        };
        eprintln!("  Hash reads:  {hash_reads}");

        match (reflink_ok, odirect_ok) {
            (Some(true), Some(true)) => {
                eprintln!(
                    "  File copy:   1. FICLONE  (Btrfs reflink — atomic, zero data movement)"
                );
                eprintln!("               2. O_DIRECT read + buffered write  (fallback)");
                eprintln!("               3. std::fs::copy / copy_file_range  (final fallback)");
            }
            (Some(true), _) => {
                eprintln!(
                    "  File copy:   1. FICLONE  (Btrfs reflink — atomic, zero data movement)"
                );
                eprintln!("               2. std::fs::copy / copy_file_range  (fallback)");
            }
            (_, Some(true)) => {
                eprintln!("  File copy:   1. O_DIRECT read + buffered write");
                eprintln!("               2. std::fs::copy / copy_file_range  (fallback)");
            }
            _ => {
                eprintln!("  File copy:   std::fs::copy / copy_file_range");
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("  Hash reads:  buffered (std::fs)");
        eprintln!("  File copy:   std::fs::copy");
    }

    eprintln!("\n{sep}\n");
}

// ── Platform helpers ──────────────────────────────────────────────────────────

/// Read the running kernel version string via `uname(2)`.
#[cfg(target_os = "linux")]
fn kernel_version() -> String {
    use std::ffi::CStr;
    let mut uts: libc::utsname = unsafe { std::mem::zeroed() };
    // SAFETY: `uname` fills the struct with null-terminated C strings on success.
    if unsafe { libc::uname(&mut uts) } == 0 {
        let release = unsafe { CStr::from_ptr(uts.release.as_ptr() as *const libc::c_char) };
        release.to_string_lossy().into_owned()
    } else {
        "unknown".to_string()
    }
}

/// Identify the filesystem type at `path` using `statfs(2)` magic numbers.
fn fs_type(path: &Path) -> String {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let cpath = match CString::new(path.as_os_str().as_bytes()) {
            Ok(c) => c,
            Err(_) => return "unknown".to_string(),
        };
        let mut buf: libc::statfs = unsafe { std::mem::zeroed() };
        // SAFETY: `cpath` is a valid null-terminated path; `buf` is zero-initialised
        // and correctly sized for the `statfs` call.
        if unsafe { libc::statfs(cpath.as_ptr(), &mut buf) } != 0 {
            return "unknown".to_string();
        }

        // Magic numbers from include/linux/magic.h
        match buf.f_type as u32 {
            0x9123_683E => "Btrfs".to_string(),
            0xEF53 => "ext4 / ext2 / ext3".to_string(),
            0x5846_5342 => "XFS".to_string(),
            0x4D44 => "FAT / VFAT".to_string(),
            0x6573_5546 => "FUSE".to_string(),
            0x0102_1994 => "tmpfs".to_string(),
            0xFF53_4D42 => "CIFS / SMB".to_string(),
            0x6969 => "NFS v2".to_string(),
            0x6E66_7364 => "NFS v4".to_string(),
            0x0072_6571 => "ReiserFS".to_string(),
            0x2011_BAB0 => "exFAT".to_string(),
            0x0000_ADF5 => "ADFS".to_string(),
            other => format!("unknown (0x{other:08X})"),
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        "n/a (non-Linux)".to_string()
    }
}

/// Probe whether `dir`'s filesystem supports `O_DIRECT` by creating a tiny
/// temp file and attempting to open it with the flag.
///
/// Returns `Some(true/false)` if the probe file could be written, `None` if not.
/// Never probes inside source directories — only call with an output path.
#[cfg(target_os = "linux")]
fn probe_direct_io(dir: &Path) -> Option<bool> {
    use std::os::unix::fs::OpenOptionsExt;

    let probe = dir.join(".awoo_probe_odirect");
    std::fs::write(&probe, b"x").ok()?;

    let supported = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(&probe)
        .is_ok();

    let _ = std::fs::remove_file(&probe);
    Some(supported)
}

/// Probe whether `dir`'s filesystem supports Btrfs reflinks via the `FICLONE` ioctl.
///
/// Returns `Some(true/false)` if probe files could be created, `None` if not.
/// Never probes inside source directories — only call with an output path.
#[cfg(target_os = "linux")]
fn probe_reflink(dir: &Path) -> Option<bool> {
    use std::fs::OpenOptions;
    use std::os::unix::io::AsRawFd;

    const FICLONE: libc::c_ulong = 0x40049409;

    let src_path = dir.join(".awoo_probe_reflink_src");
    let dst_path = dir.join(".awoo_probe_reflink_dst");

    std::fs::write(&src_path, b"x").ok()?;

    let ok = (|| -> Option<bool> {
        let src = std::fs::File::open(&src_path).ok()?;
        let dst = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&dst_path)
            .ok()?;
        // SAFETY: FICLONE is a valid ioctl; both fds are open regular files.
        let ret = unsafe { libc::ioctl(dst.as_raw_fd(), FICLONE, src.as_raw_fd()) };
        Some(ret == 0)
    })()
    .unwrap_or(false);

    let _ = std::fs::remove_file(&src_path);
    let _ = std::fs::remove_file(&dst_path);
    Some(ok)
}
