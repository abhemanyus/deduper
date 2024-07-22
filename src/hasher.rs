use base64ct::Base64UrlUnpadded;
use base64ct::Encoding;
use sha2::Digest;
use sha2::Sha256;
use std::fs::File;

pub fn file_hash(path: &str) -> String {
    let mut file = File::open(path).unwrap();
    let mut sha256 = Sha256::new();
    std::io::copy(&mut file, &mut sha256).unwrap();
    let hash = sha256.finalize();
    Base64UrlUnpadded::encode_string(&hash)
}

#[test]
fn test_file_hash() {
    let base64_hash = file_hash("/storage/Videos/2023/2023-09-01-22-49-41-343.mp4");
    assert_eq!("BrV-IyQTvSXPicvRzKjzjx00GvdnYorDD565BwgWzNs", base64_hash);
}
