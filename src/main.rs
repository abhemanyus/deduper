mod csv;
mod extractor;
mod hasher;

// use extractors::extract_timestamp;
// use hasher::file_hash;

fn main() {}

#[test]
fn test_parse_csv() {
    csv::parse_csv("pictures.csv")
        .map(|row| extractor::extract_timestamp(&row.path))
        .count();
}
