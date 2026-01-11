mod csv;
mod database;
mod extractor;
mod hasher;

use std::{
    fs::create_dir_all,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use chrono::{Datelike, Local, TimeZone};
use clap::{Parser, Subcommand};
use walkdir::WalkDir;

use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};

use parse_size::parse_size;

use crate::{
    database::{DB, LockDB},
    extractor::extract_timestamp,
};

#[cfg(unix)]
use std::os::unix::fs::symlink;

fn main() {
    let cli = Cli::parse();

    let db = DB::new(&cli.database).expect("failed to open database");

    match cli.command {
        Commands::Scan { sources, threads } => scan(sources, threads, db),
        Commands::Stats => stats(db),
        Commands::Build {
            destination,
            selector,
            split_at,
        } => build(destination, db, selector, split_at),
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
    println!("Total files: {}", db.lock().count_files().unwrap());
    println!(
        "Redundant files: {}",
        db.lock().count_redundant_files().unwrap()
    );
    Ok(())
}

fn build(
    destination: PathBuf,
    db: DB,
    selector: Option<String>,
    split_at: Option<NonZeroUsize>,
) -> Result<(), String> {
    let db = db.lock();
    let mut stmt = db
        .connection
        .prepare(if let Some(split_at) = split_at {
            println!("Splitting archive at {split_at} bytes!");
            LockDB::FIND_UNIQUE_FILES_ORDERED
        } else {
            LockDB::FIND_UNIQUE_FILES
        })
        .map_err(|e| e.to_string())?;
    let unique_files = stmt
        .query_map((), |row| {
            let ts: i64 = row.get(3)?;
            Ok(database::File {
                path: row.get(0)?,
                size_bytes: row.get(1)?,
                blake3: row.get(2)?,
                created_at: Local.timestamp_opt(ts, 0).single().unwrap(),
            })
        })
        .map_err(|e| e.to_string())?;

    let mut total_bytes = 0;
    for file in unique_files {
        let file = file.map_err(|e| e.to_string())?;
        let path = Path::new(&file.path);
        let mime_type = extractor::extract_mimetype(path);

        let timestamp = file.created_at;
        let ext = path
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("bin");

        let media_type = mime_type.type_().as_str();

        if let Some(ref selector) = selector
            && media_type != selector
        {
            continue;
        }

        total_bytes += file.size_bytes.try_into().unwrap_or(0);
        let dest_dir = if let Some(split_at) = split_at {
            destination
                .join(format!("shard_{}", (total_bytes / split_at) + 1))
                .join(media_type)
                .join(timestamp.year().to_string())
        } else {
            destination
                .join(media_type)
                .join(timestamp.year().to_string())
        };
        let time_string = timestamp.format("%d-%m-%Y_%H:%M:%S").to_string();
        let filename = format!("{}.{}", time_string, ext);
        let mut dest_path = dest_dir.join(filename);

        for i in 1..10 {
            if dest_path.exists() {
                let filename = format!("{}_{}.{}", time_string, i, ext);
                dest_path = dest_dir.join(filename);
            } else {
                break;
            }
        }

        create_dir_all(&dest_dir).map_err(|e| format!("mkdir failed: {e}"))?;

        #[cfg(unix)]
        {
            symlink(path, &dest_path).map_err(|e| {
                format!(
                    "symlink already exists or failed {}: {}",
                    dest_path.display(),
                    e
                )
            })?;
        }

        #[cfg(not(unix))]
        {
            std::fs::copy(path, &dest_path).map_err(|e| format!("copy failed: {e}"))?;
        }
    }
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
        #[arg(
            short,
            long,
            value_hint = clap::ValueHint::DirPath,
            required = true
        )]
        destination: PathBuf,
        #[arg(short, long)]
        selector: Option<String>,
        #[arg(long, value_parser = non_zero_bytes)]
        split_at: Option<NonZeroUsize>,
    },
}

fn non_zero_bytes(s: &str) -> Result<NonZeroUsize, String> {
    let val = parse_size(s).map_err(|e| e.to_string())?;
    Ok(
        NonZeroUsize::new(val.try_into().map_err(|_| "value out of bounds")?)
            .ok_or("value cannot be zero")?,
    )
}
