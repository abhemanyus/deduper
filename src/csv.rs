#[derive(Debug)]
pub struct CsvRow {
    pub path: String,
    pub hash: String,
    pub size: u64,
    pub media_type: String,
}

impl From<String> for CsvRow {
    fn from(value: String) -> Self {
        let tokens = value.split(',').collect::<Vec<&str>>();
        Self {
            path: tokens[0].trim_matches('"').to_owned(),
            hash: tokens[1].trim_matches('"').to_string(),
            size: tokens[2].trim_matches('"').parse().unwrap(),
            media_type: tokens[3].trim_matches('"').to_string(),
        }
    }
}

pub fn parse_csv(
    path: &str,
) -> std::iter::Map<
    std::io::Lines<std::io::BufReader<std::fs::File>>,
    impl FnMut(Result<String, std::io::Error>) -> CsvRow,
> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    let csv = BufReader::new(File::open(path).unwrap());
    csv.lines().map(|line| CsvRow::from(line.unwrap()))
}
