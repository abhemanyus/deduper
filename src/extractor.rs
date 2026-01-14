use std::{
    fs::{File, metadata},
    io::BufReader,
    path::Path,
};

use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime};
use exif::{In, Tag};

use ffmpeg_next as ffmpeg;
use mime_guess::{Mime, mime};
use std::sync::Once;

static FFMPEG_INIT: Once = Once::new();

pub fn extract_timestamp(path: &Path) -> Option<DateTime<Local>> {
    let mime = extract_mimetype(path);

    if mime.type_() == mime::IMAGE {
        extract_image_timestamp(path)
    } else if mime.type_() == mime::VIDEO {
        extract_video_timestamp(path)
    } else {
        None
    }
    .or_else(|| extract_filesystem_timestamp(path))
}

pub fn extract_filesystem_timestamp(path: &Path) -> Option<DateTime<Local>> {
    metadata(path)
        .ok()?
        .modified()
        .ok()
        .map(DateTime::<Local>::from)
}

pub fn extract_image_timestamp(path: &Path) -> Option<DateTime<Local>> {
    let file = File::open(path).ok()?;
    let mut buf = BufReader::new(file);
    let exif_reader = exif::Reader::new();
    let exif_data = exif_reader.read_from_container(&mut buf).ok()?;
    let field = [Tag::DateTimeOriginal, Tag::DateTimeDigitized, Tag::DateTime]
        .iter()
        .find_map(|&tag| exif_data.get_field(tag, In::PRIMARY))?;
    let datetime = match field.value {
        exif::Value::Ascii(ref vec) if !vec.is_empty() => exif::DateTime::from_ascii(&vec[0]).ok(),
        _ => None,
    }?;
    let date = NaiveDate::from_ymd_opt(
        datetime.year.into(),
        datetime.month.into(),
        datetime.day.into(),
    )?;
    let time = NaiveTime::from_hms_nano_opt(
        datetime.hour.into(),
        datetime.minute.into(),
        datetime.second.into(),
        datetime.nanosecond.unwrap_or_default(),
    )?;
    let datetime = NaiveDateTime::new(date, time);
    datetime.and_local_timezone(Local).single()
}

pub fn extract_video_timestamp(path: &Path) -> Option<DateTime<Local>> {
    init_ffmpeg();

    let ctx = ffmpeg::format::input(path).ok()?;
    let dict = ctx.metadata();
    let value = dict.get("creation_time")?;

    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.with_timezone(&Local))
}

pub fn init_ffmpeg() {
    FFMPEG_INIT.call_once(|| {
        ffmpeg::init().expect("ffmpeg init failed");
    });
}

pub fn extract_mimetype(path: &Path) -> Mime {
    mime_guess::from_path(path).first_or_octet_stream()
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use mime_guess::mime;

    use crate::extractor::{extract_image_timestamp, extract_mimetype, extract_timestamp};

    #[test]
    fn test_extract_image_timestamp() {
        extract_image_timestamp(Path::new(
            "/storage/Media/Photos/2018/2018-10-04_15:21:19_VztvDQ2lJ6RHxxr7A2ZzTg.jpg",
        ))
        .unwrap();
    }

    #[test]
    fn test_extract_video_timestamp() {
        extract_timestamp(Path::new(
            "/storage/Media/Videos/2024/2024-07-21_07:17:32_jZzCOgaj2ORYr7VZ6qEnyw.mp4",
        ))
        .unwrap();
    }

    #[test]
    fn test_extract_mimetype() {
        let mime = extract_mimetype(Path::new("video.mp4"));
        assert_eq!(mime.type_(), mime::VIDEO);
        assert_eq!(mime.subtype(), mime::MP4);
    }
}
