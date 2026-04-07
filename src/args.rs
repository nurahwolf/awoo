use anyhow::{bail, Context, Result};
use clap::{ArgAction, ArgGroup, Parser, ValueEnum};
use std::path::{Path, PathBuf};

/// Represents a parsed source specification with validated name and path.
#[derive(Debug, Clone)]
pub struct SourceSpec {
    pub name: String,
    pub path: PathBuf,
}

impl SourceSpec {
    /// Parse a source specification string in the format `Name:/path/to/dir` or `/path/to/dir`.
    pub fn parse(spec: &str) -> Result<Self> {
        let (name, path_str) = match spec.split_once(':') {
            Some((n, p)) => {
                if n.is_empty() {
                    bail!("Source name cannot be empty in specification: {}", spec);
                }
                // Validate name doesn't contain path separators
                if n.contains('/') || n.contains('\\') {
                    bail!(
                        "Source name '{}' contains invalid characters (/, \\). Use a simple label.",
                        n
                    );
                }
                (n.to_string(), p)
            }
            None => {
                let p = Path::new(spec);
                let n = p
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or(spec)
                    .to_string();
                if n.is_empty() {
                    bail!("Could not derive source name from path: {}", spec);
                }
                (n, spec)
            }
        };

        let path = std::fs::canonicalize(path_str)
            .with_context(|| format!("Failed to access source directory: {}", path_str))?;

        if !path.is_dir() {
            bail!("Source path is not a directory: {}", path.display());
        }

        Ok(SourceSpec { name, path })
    }
}

/// Verbosity level for output control.
#[derive(Debug, Default, Clone, Copy, ValueEnum)]
pub enum Verbosity {
    /// Suppress all output except errors
    Quiet,
    #[default]
    /// Standard output with progress bars
    Normal,
    /// Detailed output with additional information
    Verbose,
}

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
SOURCE FORMAT:\n\
  Sources can be specified in two formats:\n\
    1. Name:/path/to/dir   - Assign an explicit label (e.g., 'backup:/mnt/backup')\n\
    2. /path/to/dir        - Label derived from directory basename (e.g., '/data' → 'data')\n\
\n\
EXAMPLES:\n\
  awoo /home/user/data /home/user/docs\n\
  awoo production:/mnt/prod backup:/mnt/bak -o /merged/main\n\
  awoo --dry-run --verbose src1:./alpha src2:./beta\n\
\n\
Use -h for a brief summary of flags, or --help for this full description."
)]
#[command(group(
    ArgGroup::new("output_dirs")
        .args(["consolidated", "collision"])
        .multiple(true)
))]
pub struct Args {
    // ==================== Required Arguments ====================
    /// Source directories to merge.
    /// 
    /// Format: `Name:/path/to/dir` (explicit label) or `/path/to/dir` (auto-label).
    /// Multiple sources can be specified. Each source must have a unique name.
    /// 
    /// Examples:
    ///   - `backup:/mnt/backup` - labels this source as "backup"
    ///   - `/home/user/data`    - automatically labeled as "data"
    #[arg(required = true, value_name = "SOURCE", help_heading = "Required Arguments")]
    pub sources: Vec<String>,

    // ==================== Output Configuration ====================
    /// Output directory for unique (non-conflicting) files
    #[arg(
        short = 'o',
        long,
        value_name = "DIR",
        default_value_os_t = std::env::current_dir().unwrap_or_else(|_| ".".into()).join("Consolidated"),
        help_heading = "Output Configuration"
    )]
    pub consolidated: PathBuf,

    /// Output directory for conflicting files (same path, different content)
    #[arg(
        short = 'c',
        long,
        value_name = "DIR",
        default_value_os_t = std::env::current_dir().unwrap_or_else(|_| ".".into()).join("Collision"),
        help_heading = "Output Configuration"
    )]
    pub collision: PathBuf,

    // ==================== Operation Mode ====================
    /// Show what would be done without actually copying any files
    #[arg(long, help_heading = "Operation Mode")]
    pub dry_run: bool,

    /// Resume from a previous interrupted run, skipping already-processed files
    #[arg(long, help_heading = "Operation Mode")]
    pub resume: bool,

    /// Force overwrite of existing files in output directories
    #[arg(long, help_heading = "Operation Mode")]
    pub force: bool,

    // ==================== Performance & Caching ====================
    /// Disable the hash cache (re-hash all files even if previously processed)
    #[arg(long, help_heading = "Performance & Caching")]
    pub no_cache: bool,

    /// Path to the progress/cache file
    #[arg(
        long,
        value_name = "FILE",
        help_heading = "Performance & Caching",
        long_help = "Path to the progress/cache file for resuming operations.\n\
                     If not specified, defaults to <consolidated-dir>/.awoo_progress.json"
    )]
    pub progress_file: Option<PathBuf>,

    /// Number of Rayon worker threads
    #[arg(
        long,
        value_name = "NUM",
        help_heading = "Performance & Caching",
        long_help = "Number of Rayon worker threads for parallel processing.\n\
                     \n\
                     Default: number of logical CPUs\n\
                     \n\
                     Tip: For I/O-heavy workloads, over-subscribing (e.g., 2× CPU count)\n\
                     can improve throughput by keeping storage busy while some threads wait."
    )]
    pub threads: Option<usize>,

    // ==================== Output Control ====================
    /// Control verbosity level of output
    #[arg(
        short = 'v',
        long,
        value_enum,
        default_value_t = Verbosity::Normal,
        help_heading = "Output Control"
    )]
    pub verbosity: Verbosity,

    /// Suppress all output except errors (equivalent to --verbosity quiet)
    #[arg(
        short = 'q',
        long,
        action = ArgAction::SetTrue,
        conflicts_with = "verbosity",
        help_heading = "Output Control"
    )]
    pub quiet: bool,

    /// Print detailed diagnostic information and continue with the operation
    #[arg(long, help_heading = "Output Control")]
    pub debug: bool,
}

impl Args {
    /// Parse command-line arguments into Args.
    pub fn parse() -> Self {
        Parser::parse()
    }

    /// Parse command-line arguments with explicit handling of quiet flag.
    pub fn parse_from<I>(itr: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString> + Clone,
    {
        Parser::parse_from(itr)
    }

    /// Returns the effective verbosity level, considering the --quiet flag.
    pub fn verbosity(&self) -> Verbosity {
        if self.quiet {
            Verbosity::Quiet
        } else {
            self.verbosity
        }
    }

    /// Check if verbose output is enabled.
    pub fn is_verbose(&self) -> bool {
        matches!(self.verbosity(), Verbosity::Verbose)
    }

    /// Check if quiet mode is enabled.
    pub fn is_quiet(&self) -> bool {
        matches!(self.verbosity(), Verbosity::Quiet)
    }

    /// Validate the parsed arguments after parsing.
    /// 
    /// This performs runtime validation that cannot be done by clap:
    /// - Source directories exist and are accessible
    /// - Output directories don't overlap with sources
    /// - Consolidated and collision directories don't overlap
    /// - Source names are unique
    pub fn validate(&self) -> Result<Vec<SourceSpec>> {
        // Parse and validate all source specifications
        let sources: Vec<SourceSpec> = self
            .sources
            .iter()
            .map(|s| SourceSpec::parse(s))
            .collect::<Result<Vec<_>>>()?;

        // Check for duplicate source names
        use std::collections::HashSet;
        let mut seen_names = HashSet::new();
        for source in &sources {
            if !seen_names.insert(&source.name) {
                bail!(
                    "Duplicate source name '{}'. Each source must have a unique label.\n\
                     Hint: Use the Name:/path format to assign unique labels.",
                    source.name
                );
            }
        }

        // Resolve output paths for overlap checking
        let cons_abs = resolve_path(&self.consolidated);
        let coll_abs = resolve_path(&self.collision);

        // Check if consolidated and collision directories overlap
        if paths_overlap(&cons_abs, &coll_abs) {
            bail!(
                "Consolidated directory ({}) and Collision directory ({}) overlap.\n\
                 Please specify separate, non-overlapping output directories.",
                cons_abs.display(),
                coll_abs.display()
            );
        }

        // Check if any source overlaps with output directories
        for source in &sources {
            if paths_overlap(&source.path, &cons_abs) {
                bail!(
                    "Source '{}' ({}) overlaps with the Consolidated output directory ({}).\n\
                     This would cause undefined behavior. Please use separate directories.",
                    source.name,
                    source.path.display(),
                    cons_abs.display()
                );
            }
            if paths_overlap(&source.path, &coll_abs) {
                bail!(
                    "Source '{}' ({}) overlaps with the Collision output directory ({}).\n\
                     This would cause undefined behavior. Please use separate directories.",
                    source.name,
                    source.path.display(),
                    coll_abs.display()
                );
            }
        }

        Ok(sources)
    }
}

/// Returns a best-effort absolute path. Tries `canonicalize` first (resolves symlinks);
/// falls back to prepending the current working directory for paths that don't exist yet.
pub fn resolve_path(path: &Path) -> PathBuf {
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
pub fn paths_overlap(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_spec_parse_bare_path() {
        // Create a temp directory for testing
        let temp_dir = tempfile::tempdir().unwrap();
        let spec = format!("{}", temp_dir.path().display());
        let source = SourceSpec::parse(&spec).unwrap();
        
        assert_eq!(source.path, temp_dir.path().canonicalize().unwrap());
        // Name should be derived from the directory basename
        assert!(!source.name.is_empty());
    }

    #[test]
    fn test_source_spec_parse_named_path() {
        let temp_dir = tempfile::tempdir().unwrap();
        let spec = format!("mylabel:{}", temp_dir.path().display());
        let source = SourceSpec::parse(&spec).unwrap();
        
        assert_eq!(source.name, "mylabel");
        assert_eq!(source.path, temp_dir.path().canonicalize().unwrap());
    }

    #[test]
    fn test_source_spec_invalid_name_chars() {
        let result = SourceSpec::parse("bad/name:/tmp");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid characters"));
    }

    #[test]
    fn test_source_spec_nonexistent_path() {
        let result = SourceSpec::parse("/nonexistent/path/that/does/not/exist");
        assert!(result.is_err());
    }

    #[test]
    fn test_paths_overlap() {
        let a = Path::new("/foo/bar");
        let b = Path::new("/foo/bar/baz");
        assert!(paths_overlap(a, b));
        assert!(paths_overlap(b, a));

        let c = Path::new("/foo/qux");
        assert!(!paths_overlap(a, c));
    }
}
