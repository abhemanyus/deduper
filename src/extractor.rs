use std::{
    fs::{metadata, File},
    io::BufReader,
    time::UNIX_EPOCH,
};

use chrono::{DateTime, Local, NaiveDateTime, TimeZone};
use exif::{In, Tag};

use ffmpeg_next as ffmpeg;

pub fn extract_timestamp(path: &str) -> DateTime<Local> {
    let mimetype = extract_mimetype(path);
    if mimetype.starts_with("image/") {
        if let Some(timestamp) = extract_image_timestamp(path) {
            timestamp
        } else {
            extract_filesystem_timestamp(path)
        }
    } else if mimetype.starts_with("video/") {
        if let Some(timestamp) = extract_video_timestamp(path) {
            timestamp
        } else {
            extract_filesystem_timestamp(path)
        }
    } else {
        extract_filesystem_timestamp(path)
    }
}

fn extract_filesystem_timestamp(path: &str) -> DateTime<Local> {
    metadata(path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|sys_time| sys_time.duration_since(UNIX_EPOCH).ok())
        .and_then(|duration| {
            Local
                .timestamp_opt(duration.as_secs() as i64, duration.subsec_nanos())
                .single()
        })
        .unwrap()
}

fn extract_image_timestamp(path: &str) -> Option<DateTime<Local>> {
    File::open(path)
        .ok()
        .map(|file| BufReader::new(file))
        .and_then(|mut buf| {
            let exif_reader = exif::Reader::new();
            exif_reader.read_from_container(&mut buf).ok()
        })
        .and_then(|exif_data| exif_data.get_field(Tag::DateTime, In::PRIMARY).cloned())
        .map(|field| field.display_value().with_unit(&field).to_string())
        .and_then(|date_string| {
            NaiveDateTime::parse_from_str(&date_string, "%Y:%m:%d %H:%M:%S").ok()
        })
        .and_then(|date_time| date_time.and_local_timezone(Local).single())
}

// fn extract_image_timestamp(path: &str) -> Option<DateTime<Local>> {
//     rexiv2::Metadata::new_from_path(path)
//         .ok()
//         .and_then(|meta| meta.get_tag_string("Exif.Image.DateTime").ok())
// .and_then(|date_string| {
//     NaiveDateTime::parse_from_str(&date_string, "%Y:%m:%d %H:%M:%S").ok()
// })
// .and_then(|date_time| date_time.and_local_timezone(Local).single())
// }

fn extract_video_timestamp(path: &str) -> Option<DateTime<Local>> {
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

// fn extract_video_timestamp(path: &str) -> Option<DateTime<Local>> {
//     std::process::Command::new("ffprobe")
//         .args(&[
//             "-v",
//             "quiet",
//             "-print_format",
//             "csv=nk=1:p=0",
//             "-show_entries",
//             "format_tags=creation_time",
//         ])
//         .arg(path)
//         .output()
//         .ok()
//         .and_then(|output| String::from_utf8(output.stdout).ok())
//         .and_then(|date_string| {
//             NaiveDateTime::parse_from_str(&date_string.trim(), "%Y-%m-%dT%H:%M:%S%.f%Z").ok()
//         })
//         .and_then(|date_time| date_time.and_local_timezone(Local).single())
// }

fn extract_mimetype(path: &str) -> String {
    mime_guess::from_path(path)
        .first_or_octet_stream()
        .to_string()
}

// fn extract_mimetype(path: &str) -> String {
//     std::process::Command::new("file")
//         .args(&["-b", "--mime-type"])
//         .arg(path)
//         .output()
//         .ok()
//         .and_then(|output| String::from_utf8(output.stdout).ok())
//         .and_then(|mimetype| Some(mimetype.trim().to_string()))
//         .unwrap()
// }

#[test]
fn test_extract_image_timestamp() {
    extract_timestamp("/storage/Pictures/scans/blood_12_08_2023.jpeg");
}

#[test]
fn test_extract_video_timestamp() {
    extract_timestamp("/storage/Videos/2023/2023-09-01-22-49-41-343.mp4");
}

#[test]
fn test_extract_mimetype() {
    assert_eq!(
        "video/mp4",
        extract_mimetype("/storage/Videos/2023/2023-09-01-22-49-41-343.mp4")
    );
}
