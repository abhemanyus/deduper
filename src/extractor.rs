use std::{
    fs::{metadata, File},
    io::BufReader,
    path::Path,
    time::UNIX_EPOCH,
};

use chrono::{DateTime, Local, NaiveDateTime, TimeZone};
use exif::{In, Tag};

use ffmpeg_next as ffmpeg;
use mime_guess::Mime;

// pub fn extract_timestamp(path: &str) -> DateTime<Local> {
//     let mimetype = extract_mimetype(path);
//     if mimetype.starts_with("image/") {
//         if let Some(timestamp) = extract_image_timestamp(path) {
//             timestamp
//         } else {
//             extract_filesystem_timestamp(path)
//         }
//     } else if mimetype.starts_with("video/") {
//         if let Some(timestamp) = extract_video_timestamp(path) {
//             timestamp
//         } else {
//             extract_filesystem_timestamp(path)
//         }
//     } else {
//         extract_filesystem_timestamp(path)
//     }
// }

pub fn extract_filesystem_timestamp(path: &Path) -> Option<DateTime<Local>> {
    metadata(path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|sys_time| sys_time.duration_since(UNIX_EPOCH).ok())
        .and_then(|duration| {
            Local
                .timestamp_opt(duration.as_secs() as i64, duration.subsec_nanos())
                .single()
        })
}

pub fn extract_image_timestamp(path: &Path) -> Option<DateTime<Local>> {
    File::open(path)
        .ok()
        .map(|file| BufReader::new(file))
        .and_then(|mut buf| {
            let exif_reader = exif::Reader::new();
            exif_reader.read_from_container(&mut buf).ok()
        })
        .and_then(|exif_data| {
            for tag in [Tag::DateTime, Tag::DateTimeOriginal, Tag::DateTimeDigitized] {
                if let Some(field) = exif_data.get_field(tag, In::PRIMARY) {
                    return Some(field.clone());
                }
            }
            None
        })
        .map(|field| field.display_value().with_unit(&field).to_string())
        .and_then(|date_string| {
            for format in ["%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"] {
                if let Ok(date_time) = NaiveDateTime::parse_from_str(&date_string, format) {
                    return Some(date_time);
                }
            }
            None
        })
        .and_then(|date_time| date_time.and_local_timezone(Local).single())
}

pub fn extract_video_timestamp(path: &Path) -> Option<DateTime<Local>> {
    ffmpeg::init().expect("could not initialize ffmpeg");

    ffmpeg::format::input(path)
        .ok()
        .and_then(|context| {
            context
                .metadata()
                .get("creation_time")
                .map(|str| str.to_owned())
        })
        .and_then(|date_string| {
            NaiveDateTime::parse_from_str(&date_string.trim(), "%Y-%m-%dT%H:%M:%S%.f%Z").ok()
        })
        .and_then(|date_time| date_time.and_local_timezone(Local).single())
}

pub fn extract_mimetype(path: &Path) -> Mime {
    mime_guess::from_path(path).first_or_octet_stream()
}

#[test]
fn test_extract_image_timestamp() {
    extract_image_timestamp(Path::new("/storage/Backup/2019/20190901_070202.jpg")).unwrap();
}

// #[test]
// fn test_extract_video_timestamp() {
//     extract_timestamp("/storage/Videos/2023/2023-09-01-22-49-41-343.mp4");
// }

#[test]
fn test_extract_mimetype() {
    assert_eq!(
        "video/mp4",
        extract_mimetype(Path::new(
            "/storage/Videos/2023/2023-09-01-22-49-41-343.mp4"
        ))
    );
}
