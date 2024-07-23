mod csv;
mod extractor;
mod hasher;

use std::path::PathBuf;

use clap::Parser;

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
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "DIR", value_hint = clap::ValueHint::DirPath)]
    sources: Vec<PathBuf>,
    #[arg(short, long, value_name = "DIR", value_hint = clap::ValueHint::DirPath)]
    destination: PathBuf,
}

#[test]
fn test_parse_csv() {
    csv::parse_csv("pictures.csv")
        .map(|row| extractor::extract_timestamp(&row.path))
        .count();
}
