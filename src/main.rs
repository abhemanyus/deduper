mod database;
mod extractor;
mod hasher;
mod transcoder;

use std::{
    fs::create_dir_all,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Mutex,
    thread::scope,
};

use chrono::Datelike;
use clap::{Parser, Subcommand};
use walkdir::WalkDir;

use parse_size::parse_size;

use crate::{
    database::{DB, LockDB},
    extractor::extract_timestamp,
    transcoder::transcode,
};

#[cfg(unix)]
use std::os::unix::fs::symlink;

fn main() {
    let cli = Cli::parse();

    let db = DB::new(&cli.database).expect("failed to open database");

    match cli.command {
        Commands::Scan { sources, threads } => scan(sources, threads, db),
        Commands::Stats => stats(db),
        Commands::Optimize { temp } => optimize(temp, db),
        Commands::Build {
            destination,
            selector,
            split_at,
        } => build(destination, db, selector, split_at),
    }
    .unwrap();
}

fn optimize(temp: PathBuf, db: DB) -> Result<(), String> {
    let rows = db.lock().mark_original_files().map_err(|e| e.to_string())?;
    println!("Rows marked: {rows}");
    create_dir_all(&temp).map_err(|e| e.to_string())?;
    println!("Directory created");
    let db_lock = db.lock();
    let mut stmt = db_lock
        .connection
        .prepare(LockDB::FIND_UNOPTIMIZED_VIDEOS)
        .map_err(|e| e.to_string())?;
    let to_optimize = stmt
        .query_map((), |row| database::File::try_from(row))
        .map_err(|e| e.to_string())?;
    for file in to_optimize {
        let Ok(mut file) = file else {
            continue;
        };

        let filename = format!("{}_{}.mkv", file.blake3, file.size_bytes);
        let output_file = temp.join(filename);

        let Ok(_) = transcode(&Path::new(&file.path), &output_file) else {
            eprintln!("failed to transcode file: {}", &file.path);
            continue;
        };

        let new_size = std::fs::metadata(&output_file)
            .map_err(|e| e.to_string())?
            .len();

        file.size_bytes = new_size.try_into().unwrap();
        file.optimized = Some(output_file.to_str().unwrap().to_string());

        db_lock.insert_file(&file).unwrap();
    }
    Ok(())
}

fn scan(sources: Vec<PathBuf>, threads: usize, db: DB) -> Result<(), String> {
    let (sender, receiver) = crossbeam::channel::bounded::<PathBuf>(256);
    let sources = Mutex::new(sources);
    scope(|s| {
        let _hash_pool = (0..num_cpus::get())
            .map(|_| {
                s.spawn(|| -> Result<(), String> {
                    let db = db.clone();
                    for path in &receiver {
                        if let Err(err) = process_file(&path, &db) {
                            eprintln!("❌ {}: {err}", path.display());
                        }
                    }
                    Ok(())
                })
            })
            .collect::<Vec<_>>();
        scope(|s| {
            let _io_pool = (0..threads)
                .map(|_| {
                    s.spawn(|| -> Result<(), String> {
                        loop {
                            let Some(source) = sources.lock().map_err(|e| e.to_string())?.pop()
                            else {
                                break Ok(());
                            };
                            WalkDir::new(source)
                                .follow_links(false)
                                .into_iter()
                                .filter_map(Result::ok)
                                .filter(|e| e.file_type().is_file())
                                .for_each(|entry| {
                                    if let Err(err) = sender.send(entry.into_path()) {
                                        eprintln!("❌ {}: {err}", err.0.display());
                                    }
                                });
                        }
                    })
                })
                .collect::<Vec<_>>();
        });
        drop(sender);
    });
    Ok(())
}

fn stats(db: DB) -> Result<(), String> {
    println!("Total files: {}", db.lock().count_files().unwrap());
    println!(
        "Redundant files: {}",
        db.lock().count_redundant_files().unwrap()
    );
    println!(
        "Original files marked: {}",
        db.lock().count_original_files().unwrap()
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
        .query_map((), |row| database::File::try_from(row))
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

fn process_file(path: &Path, db: &DB) -> Result<(), String> {
    let timestamp = extract_timestamp(path).ok_or("missing timestamp")?;
    let size_bytes = std::fs::metadata(path).map_err(|e| e.to_string())?.len();
    let hash = hasher::file_hash(path).ok_or("hashing failed")?;
    let db = db.lock();
    let mime_type = extractor::extract_mimetype(path);

    db.insert_file(&database::File {
        path: path.display().to_string(),
        size_bytes: size_bytes.try_into().unwrap_or(0),
        blake3: hash,
        created_at: timestamp,
        optimized: None,
        is_original: false,
        media_type: mime_type.type_().to_string(),
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

    Optimize {
        #[arg(
            short,
            long,
            value_hint = clap::ValueHint::DirPath,
        )]
        temp: PathBuf,
    },

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
