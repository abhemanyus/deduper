use std::path::Path;

pub fn file_hash(path: &Path) -> Option<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update_mmap_rayon(path).ok()?;
    let hash = hasher.finalize();
    Some(hash.to_hex().to_string())
}

#[test]
fn test_file_hash() {
    let base64_hash = file_hash(Path::new(
        "/storage/Videos/2023/2023-09-01-22-49-41-343.mp4",
    ));
    assert_eq!(
        "BrV-IyQTvSXPicvRzKjzjx00GvdnYorDD565BwgWzNs",
        base64_hash.unwrap()
    );
}
