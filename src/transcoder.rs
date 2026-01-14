use std::{path::Path, process::Command};

use ffmpeg_next::{Error, codec, format, media};

use crate::extractor::init_ffmpeg;
pub fn transcode(input_file: &Path, output_file: &Path) -> Result<(), String> {
    let output_process = Command::new("ffmpeg")
        .args([
            "-y", // overwrite output
            "-i",
            input_file.to_str().unwrap(), // input file
            "-crf",
            "35",
            "-preset",
            "8",
            "-c:v",
            "libsvtav1", // video codec
            "-c:a",
            "copy",                         // audio codec
            output_file.to_str().unwrap(), // output file
        ])
        .output()
        .map_err(|e| format!("Failed to start ffmpeg: {e}"))?;

    if output_process.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output_process.stderr);
        Err(format!(
            "ffmpeg failed with status {}\n{}",
            output_process.status, stderr
        ))
    }
}

pub fn find_codec(path: &Path) -> Result<codec::Id, Error> {
    init_ffmpeg();

    let input = format::input(path)?;
    let video_stream = input
        .streams()
        .best(media::Type::Video)
        .ok_or(Error::InvalidData)?;

    let decoder = video_stream.parameters().id();

    return Ok(decoder);
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::transcoder::{find_codec, transcode};

    #[test]
    fn test_video_format() {
        let video_path = Path::new("/home/abhe/Videos/output2.mov");
        find_codec(video_path).unwrap();
    }

    #[test]
    fn test_video_transcode() {
        let video_path = Path::new("/home/abhe/Videos/output2.mov");
        let out_path = Path::new("./test_out.mkv");
        transcode(video_path, out_path).unwrap();
    }
}
