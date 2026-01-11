mod csv;
mod database;
mod extractor;
mod hasher;

use std::{
    fs::create_dir_all,
    path::{Path, PathBuf},
};

use chrono::Datelike;
use clap::{Parser, Subcommand};
use walkdir::WalkDir;

use rayon::{prelude::*, ThreadPool, ThreadPoolBuilder};

use crate::{database::DB, extractor::extract_timestamp};

#[cfg(unix)]
use std::os::unix::fs::symlink;

fn main() {
    let cli = Cli::parse();

    let db = DB::new(&cli.database).expect("failed to open database");

    match cli.command {
        Commands::Scan { sources, threads } => scan(sources, threads, db),
        Commands::Stats => stats(db),
        Commands::Build { destination } => build(destination, db),
    }
    .unwrap();
}

fn scan(sources: Vec<PathBuf>, threads: usize, db: DB) -> Result<(), String> {
    let io_pool = ThreadPoolBuilder::new()
        .num_threads(threads) // tune this
        .thread_name(|i| format!("worker-{i}"))
        .build()
        .expect("failed to build thread pool");

    let hash_pool = ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .thread_name(|i| format!("hash-{i}"))
        .build()
        .expect("failed to build hash pool");
    io_pool.install(|| run(sources, &hash_pool, db));
    Ok(())
}

fn stats(db: DB) -> Result<(), String> {
    println!(
        "Redundant files: {}",
        db.lock().count_redundant_files().unwrap()
    );
    Ok(())
}

fn build(destination: PathBuf, db: DB) -> Result<(), String> {
    // let ext = path
    //     .extension()
    //     .and_then(|ext| ext.to_str())
    //     .unwrap_or("bin");

    // let mime_type = extractor::extract_mimetype(path);
    // let dest_dir = destination
    //     .join(mime_type.type_().as_str())
    //     .join(timestamp.year().to_string());
    // let filename = format!("{}_{}.{}", timestamp.format("%F_%X").to_string(), hash, ext);
    // let dest_path = dest_dir.join(filename);
    Ok(())
}

fn run(sources: Vec<PathBuf>, hash_pool: &ThreadPool, db: DB) {
    eprintln!(
        "Sources:\n\t{}",
        sources
            .iter()
            .map(|s| s.display().to_string())
            .collect::<Vec<_>>()
            .join("\n\t")
    );

    sources
        .par_iter()
        .flat_map(|source| {
            WalkDir::new(source)
                .follow_links(false)
                .into_iter()
                .filter_map(Result::ok)
                .filter(|e| e.file_type().is_file())
                .par_bridge()
        })
        .for_each(|entry| {
            if let Err(err) = process_file(entry.path(), hash_pool, db.clone()) {
                eprintln!("❌ {}: {err}", entry.path().display());
            }
        });
}

fn process_file(path: &Path, hash_pool: &ThreadPool, db: DB) -> Result<(), String> {
    let timestamp = extract_timestamp(path).ok_or("missing timestamp")?;
    let size_bytes = std::fs::metadata(path).map_err(|e| e.to_string())?.len();
    let hash = hash_pool.install(|| hasher::file_hash(path).ok_or("hashing failed"))?;
    let db = db.lock();

    db.insert_file(&database::File {
        path: path.display().to_string(),
        size_bytes: size_bytes.try_into().unwrap_or(0),
        blake3: hash,
        created_at: timestamp,
    })
    .map_err(|e| e.to_string())?;

    Ok(())
}

#[derive(Parser)]
#[command(name = "dedup")]
#[command(about = "File deduplication tool")]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(short, long, value_hint = clap::ValueHint::FilePath, required = true)]
    database: PathBuf,
}

#[derive(Subcommand)]
enum Commands {
    Scan {
        #[arg(
        short,
        long,
        value_hint = clap::ValueHint::DirPath,
        num_args = 1..,
        required = true
    )]
        sources: Vec<PathBuf>,
        #[arg(long, default_value_t = 4)]
        threads: usize,
    },

    Stats,

    Build {
        #[arg(short, long, value_hint = clap::ValueHint::DirPath, required = true)]
        destination: PathBuf,
    },
}
