use std::error::Error;

use chrono::{DateTime, FixedOffset};
use nom_exif::{Exif, ExifIter, ExifTag, MediaParser, MediaSource, TrackInfo, TrackInfoTag};

pub fn extract_date(file_path: &str) -> Result<DateTime<FixedOffset>, Box<dyn Error>> {
    let mut parser = MediaParser::new();
    let ms = MediaSource::file_path(file_path)?;
    if ms.has_exif() {
        let iter: ExifIter = parser.parse(ms)?;
        let info: Exif = iter.into();
        let time_tag = info.get(ExifTag::DateTimeOriginal).unwrap();
        let time_stamp = time_tag.as_time().unwrap();
        return Ok(time_stamp);
    } else if ms.has_track() {
        let info: TrackInfo = parser.parse(ms)?;
        let time_tag = info.get(TrackInfoTag::CreateDate).unwrap();
        let time_stamp = time_tag.as_time().unwrap();
        return Ok(time_stamp);
    }
    unimplemented!();
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
