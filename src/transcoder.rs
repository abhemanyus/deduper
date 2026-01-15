use std::{path::Path, process::Command};

use ffmpeg_next::{Error, codec, format, media};

use crate::extractor::init_ffmpeg;
pub fn transcode(input_file: &Path, output_file: &Path) -> Result<(), String> {
    if !input_file.exists() {
        return Err("input video does not exist")?;
    }
    let output_process = Command::new("ffmpeg")
        .args([
            "-y", // overwrite output
            "-i",
            input_file.to_str().unwrap(), // input file
            "-vf",
            "format=yuv420p",
            "-crf",
            "35",
            "-preset",
            "8",
            "-c:v",
            "libsvtav1", // video codec
            "-c:a",
            "copy",                        // audio codec
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

pub fn find_bitrate(path: &Path) -> Result<usize, Error> {
    init_ffmpeg();

    let input = format::input(path)?;
    let video_stream = input
        .streams()
        .best(media::Type::Video)
        .ok_or(Error::InvalidData)?;

    let context = codec::context::Context::from_parameters(video_stream.parameters())?;
    let decoder = context.decoder().video()?;

    let bit_rate = decoder.bit_rate();

    return Ok(bit_rate);
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::transcoder::{find_bitrate, transcode};

    #[test]
    fn test_video_format() {
        let video_path = Path::new("/home/abhe/Videos/output2.mov");
        println!("Bitrate: {}", find_bitrate(video_path).unwrap());
    }

    #[test]
    fn test_video_transcode() {
        let video_path = Path::new("/home/abhe/Videos/output2.mov");
        let out_path = Path::new("./test_out.mkv");
        transcode(video_path, out_path).unwrap();
    }
}
