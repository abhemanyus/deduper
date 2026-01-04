mod csv;
mod extractor;
mod hasher;

use std::{
    fs::create_dir_all,
    path::{Path, PathBuf},
};

use chrono::Datelike;
use clap::Parser;
use crossbeam::channel::{bounded, Receiver};
use walkdir::WalkDir;

use rayon::{prelude::*, ThreadPool, ThreadPoolBuilder};

use crate::extractor::extract_timestamp;

#[cfg(unix)]
use std::os::unix::fs::symlink;

fn main() {
    let cli = Cli::parse();
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

    io_pool.install(|| run(cli, &hash_pool));
}

fn run(cli: Cli, hash_pool: &ThreadPool) {
    let (tx, rx) = bounded::<WorkItem>(256);

    for _ in 0..hash_pool.current_num_threads() {
        let rx = rx.clone();
        let destination = cli.destination.clone();
        let dry_run = cli.dry_run;

        hash_pool.spawn(move || hash_worker(rx, destination, dry_run));
    }
    eprintln!(
        "Sources:\n\t{}",
        cli.sources
            .iter()
            .map(|s| s.display().to_string())
            .collect::<Vec<_>>()
            .join("\n\t")
    );
    eprintln!("Destination: {}", cli.destination.display());

    cli.sources.par_iter().for_each(|source| {
        WalkDir::new(source)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file())
            .for_each(|entry| {
                // blocks if channel is full
                let _ = tx.send(WorkItem {
                    path: entry.path().to_path_buf(),
                });
            });
    });

    drop(tx); // signal completion
}

struct WorkItem {
    path: PathBuf,
}

fn hash_worker(rx: Receiver<WorkItem>, destination: PathBuf, dry_run: bool) {
    for item in rx {
        if let Err(err) = process_file(&item.path, &destination, dry_run) {
            eprintln!("❌ {}: {err}", item.path.display());
        }
    }
}

fn process_file(path: &Path, destination: &Path, dry_run: bool) -> Result<(), String> {
    let mime_type = extractor::extract_mimetype(path);

    let timestamp = extract_timestamp(path).ok_or("missing timestamp")?;
    let hash = hasher::file_hash(path).ok_or("hashing failed")?;
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
}
