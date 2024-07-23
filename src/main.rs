mod csv;
mod extractor;
mod hasher;

use std::{fs::create_dir_all, os::unix::fs::symlink, path::PathBuf};

use chrono::Datelike;
use clap::Parser;
use mime_guess::mime;
use walkdir::WalkDir;

fn main() {
    let cli = Cli::parse();
    println!(
        "sources: \n\t{}",
        cli.sources
            .iter()
            .map(|s| s.to_string_lossy())
            .collect::<Vec<_>>()
            .join("\n\t")
    );
    println!("destination: {}", cli.destination.to_string_lossy());
    for source in cli.sources {
        for entry in WalkDir::new(source)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.metadata().ok().map(|e| e.is_file()).unwrap_or_default())
        {
            let mime_type = extractor::extract_mimetype(entry.path());

            let (timestamp, category) = match mime_type.type_() {
                mime::IMAGE => (extractor::extract_image_timestamp(entry.path()), "Photos"),
                mime::VIDEO => (extractor::extract_video_timestamp(entry.path()), "Videos"),
                other => {
                    println!(
                        "'{}' not supported: {}",
                        other,
                        entry.path().to_string_lossy()
                    );
                    continue;
                }
            };

            let timestamp = match timestamp {
                Some(timestamp) => timestamp,
                None => {
                    println!(
                        "using filesystem timestamp for {}",
                        entry.path().to_string_lossy()
                    );
                    match extractor::extract_filesystem_timestamp(entry.path()) {
                        Some(timestamp) => timestamp,
                        None => {
                            println!(
                                "failed to get timestamp for {}",
                                entry.path().to_string_lossy()
                            );
                            continue;
                        }
                    }
                }
            };

            let Some(hash) = hasher::file_hash(entry.path()) else {
                println!(
                    "failed to get file hash for {}",
                    entry.path().to_string_lossy()
                );
                continue;
            };

            let ext = entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .unwrap_or_default();

            let dest_dir_path = cli
                .destination
                .join(category)
                .join(timestamp.year().to_string());
            let dest_path = dest_dir_path.join(format!(
                "{}_{}.{}",
                timestamp.format("%F_%X").to_string(),
                hash,
                ext
            ));
            if create_dir_all(&dest_dir_path).is_err() {
                println!(
                    "failed to create directory {} for {}",
                    dest_dir_path.to_string_lossy(),
                    entry.path().to_string_lossy(),
                );
            };
            if let Err(_) = symlink(entry.path(), dest_path) {
                println!("link already exists for {}", entry.path().to_string_lossy());
                continue;
            };
        }
    }
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "DIR", value_hint = clap::ValueHint::DirPath)]
    sources: Vec<PathBuf>,
    #[arg(short, long, value_name = "DIR", value_hint = clap::ValueHint::DirPath)]
    destination: PathBuf,
}
