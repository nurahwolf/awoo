use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "awoo",
    version,
    about = "High-performance Btrfs merge with BLAKE3 & parallel I/O",
    long_about = "awoo merges multiple Btrfs subvolumes into a single consolidated volume.\n\
\n\
Files with identical relative paths are compared by BLAKE3 hash:\n\
  • Identical content  → copied once to Consolidated/\n\
  • Differing content  → each version placed in Collision/<source-name>/\n\
\n\
Source directories are never modified. All copies use Btrfs reflinks\n\
(FICLONE ioctl) where available, falling back to Direct I/O or a\n\
standard copy on non-Btrfs filesystems.\n\
\n\
Use -h for a brief summary of flags, or --help for this full description."
)]
pub struct Args {
    /// Source directories. Accepts either `Name:/path/to/dir` (explicit label)
    /// or a bare `/path/to/dir` (label derived from the directory's basename).
    #[arg(required = true)]
    pub sources: Vec<String>,

    /// Output directory for unique consolidated files
    #[arg(short = 'o', long, default_value = "./Consolidated")]
    pub consolidated: PathBuf,

    /// Output directory for conflicting files
    #[arg(short = 'c', long, default_value = "./Collision")]
    pub collision: PathBuf,

    /// Show what would be done without copying
    #[arg(long)]
    pub dry_run: bool,

    /// Resume from a previous interrupted run, skipping already-processed files
    #[arg(long)]
    pub resume: bool,

    /// Path to the progress/cache file (default: <consolidated>/.awoo_progress.json)
    #[arg(long)]
    pub progress_file: Option<PathBuf>,

    /// Print filesystem, I/O capability, and copy-strategy diagnostics then continue.
    #[arg(long)]
    pub debug: bool,

    /// Number of Rayon worker threads (default: logical CPU count).
    /// Over-subscribing (e.g. 2× CPU count) can improve throughput for
    /// I/O-heavy workloads by keeping storage busy while some threads wait.
    #[arg(long)]
    pub threads: Option<usize>,
}
