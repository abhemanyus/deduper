use std::{error::Error, process::Command};

use chrono::NaiveDateTime;

pub fn extract_date(file_path: &str) -> Result<NaiveDateTime, Box<dyn Error>> {
    let output = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            "-show_entries",
            "format_tags=creation_time",
            file_path,
        ])
        .output()?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr))?;
    }
    let time_string = String::from_utf8_lossy(&output.stdout);
    let time = NaiveDateTime::parse_from_str(&time_string.trim(), "%Y-%m-%dT%H:%M:%S%.6fZ")?;
    Ok(time)
}

#[cfg(test)]
mod test {
    use chrono::NaiveDateTime;

    use crate::extract_date;

    #[test]
    fn test_time_parse() {
        NaiveDateTime::parse_from_str("2024-07-21T07:17:32.000000Z", "%Y-%m-%dT%H:%M:%S%.6fZ")
            .unwrap();
    }

    #[test]
    fn test_time_extract() {
        extract_date("example.mp4").unwrap();
        extract_date("example.jpg").unwrap();
    }
}
