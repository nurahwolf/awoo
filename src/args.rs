use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "awoo",
    about = "High-performance Btrfs merge with BLAKE3 & parallel I/O"
)]
pub struct Args {
    /// Source directories in format Name:/absolute/path
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
}
