mod csv;
mod extractor;
mod hasher;

use std::{
    fs::create_dir_all,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use chrono::Datelike;
use clap::Parser;
use walkdir::WalkDir;

use rayon::{prelude::*, ThreadPool, ThreadPoolBuilder};

use crate::extractor::extract_timestamp;

#[cfg(unix)]
use std::os::unix::fs::symlink;

fn main() {
    let cli = Cli::parse();

    let conn = rusqlite::Connection::open(&cli.database).expect("failed to open database");
    let conn = Arc::new(Mutex::new(conn));

    let io_pool = ThreadPoolBuilder::new()
        .num_threads(cli.threads) // tune this
        .thread_name(|i| format!("worker-{i}"))
        .build()
        .expect("failed to build thread pool");

    let hash_pool = ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .thread_name(|i| format!("hash-{i}"))
        .build()
        .expect("failed to build hash pool");

    io_pool.install(|| run(cli, &hash_pool, conn));
}

type Connection = Arc<Mutex<rusqlite::Connection>>;

fn run(cli: Cli, hash_pool: &ThreadPool, conn: Connection) {
    eprintln!(
        "Sources:\n\t{}",
        cli.sources
            .iter()
            .map(|s| s.display().to_string())
            .collect::<Vec<_>>()
            .join("\n\t")
    );
    eprintln!("Destination: {}", cli.destination.display());

    cli.sources
        .par_iter()
        .flat_map(|source| {
            WalkDir::new(source)
                .follow_links(false)
                .into_iter()
                .par_bridge()
                .filter_map(Result::ok)
                .filter(|e| e.file_type().is_file())
        })
        .for_each(|entry| {
            if let Err(err) = process_file(
                entry.path(),
                &cli.destination,
                cli.dry_run,
                hash_pool,
                conn.clone(),
            ) {
                eprintln!("❌ {}: {err}", entry.path().display());
            }
        });
}

fn process_file(
    path: &Path,
    destination: &Path,
    dry_run: bool,
    hash_pool: &ThreadPool,
    conn: Connection,
) -> Result<(), String> {
    let mime_type = extractor::extract_mimetype(path);

    let timestamp = extract_timestamp(path).ok_or("missing timestamp")?;
    let hash = hash_pool.install(|| hasher::file_hash(path).ok_or("hashing failed"))?;
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("bin");

    let dest_dir = destination
        .join(mime_type.type_().as_str())
        .join(timestamp.year().to_string());
    let filename = format!("{}_{}.{}", timestamp.format("%F_%X").to_string(), hash, ext);
    let dest_path = dest_dir.join(filename);

    if dry_run {
        eprintln!("[DRY-RUN] {} → {}", path.display(), dest_path.display());
        return Ok(());
    }
    create_dir_all(&dest_dir).map_err(|e| format!("mkdir failed: {e}"))?;

    #[cfg(unix)]
    {
        symlink(path, &dest_path).map_err(|_| "symlink already exists or failed")?;
    }

    #[cfg(not(unix))]
    {
        std::fs::copy(path, &dest_path).map_err(|e| format!("copy failed: {e}"))?;
    }
    Ok(())
}

#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    #[arg(
        short,
        long,
        value_hint = clap::ValueHint::DirPath,
        num_args = 1..,
        required = true
    )]
    sources: Vec<PathBuf>,

    #[arg(short, long, value_hint = clap::ValueHint::DirPath, required = true)]
    destination: PathBuf,

    #[arg(long, default_value_t = 4)]
    threads: usize,

    #[arg(long, default_value_t = false)]
    dry_run: bool,

    #[arg(short, long, value_hint = clap::ValueHint::FilePath, required = true)]
    database: PathBuf,
}
