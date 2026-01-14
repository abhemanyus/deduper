use std::{
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

use chrono::{DateTime, Local, TimeZone};
use rusqlite::Row;

#[derive(Clone)]
pub struct DB {
    connection: Arc<Mutex<rusqlite::Connection>>,
}

pub struct LockDB<'a> {
    pub connection: MutexGuard<'a, rusqlite::Connection>,
}

#[derive(Clone, Debug)]
pub struct File {
    pub path: String,
    pub size_bytes: i64,
    pub blake3: String,
    pub created_at: DateTime<Local>,
    pub optimized: Option<String>,
    pub is_original: bool,
    pub media_type: String,
}

impl TryFrom<&Row<'_>> for File {
    type Error = rusqlite::Error;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        let ts: i64 = row.get("created_at")?;
        Ok(File {
            path: row.get("path")?,
            size_bytes: row.get("size_bytes")?,
            blake3: row.get("blake3")?,
            created_at: Local.timestamp_opt(ts, 0).single().unwrap(),
            optimized: row.get("optimized")?,
            is_original: row.get("is_original")?,
            media_type: row.get("media_type")?,
        })
    }
}

impl DB {
    pub fn new(path: &Path) -> Result<Self, rusqlite::Error> {
        let conn = rusqlite::Connection::open(path)?;
        conn.execute_batch(Self::CREATE_TABLE_FILES)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn lock(&self) -> LockDB<'_> {
        LockDB {
            connection: self.connection.lock().unwrap(),
        }
    }

    const CREATE_TABLE_FILES: &'static str = r#"
        CREATE TABLE IF NOT EXISTS files (
            path        TEXT PRIMARY KEY,
            size_bytes  INTEGER NOT NULL CHECK (size_bytes >= 0),
            blake3      TEXT    NOT NULL,
            created_at  INTEGER NOT NULL,
            optimized   TEXT,
            is_original INTEGER NOT NULL DEFAULT 0,
            media_type  TEXT    NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_files_blake3 ON files (blake3);
        CREATE INDEX IF NOT EXISTS idx_files_created ON files (created_at);
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_original_per_group ON files (blake3, size_bytes) WHERE is_original = 1;
    "#;
}

impl<'a> LockDB<'a> {
    pub fn insert_file(&self, file: &File) -> Result<(), rusqlite::Error> {
        self.connection.execute(
            Self::INSERT_FILE,
            (
                &file.path,
                file.size_bytes,
                &file.blake3,
                file.created_at.timestamp(),
                &file.optimized,
                file.is_original,
                &file.media_type,
            ),
        )?;
        Ok(())
    }

    pub fn count_files(&self) -> Result<i64, rusqlite::Error> {
        self.connection
            .prepare_cached(Self::COUNT_FILES)?
            .query_one((), |f| f.get(0))
    }

    pub fn count_redundant_files(&self) -> Result<i64, rusqlite::Error> {
        self.connection
            .prepare_cached(Self::COUNT_REDUNDANT_FILES)?
            .query_one((), |f| f.get(0))
    }

    pub fn find_dup_files(
        &self,
        blake3: &str,
        size_bytes: i64,
    ) -> Result<Vec<File>, rusqlite::Error> {
        self.connection
            .prepare_cached(Self::FIND_DUP_FILES)?
            .query_map((blake3, size_bytes), |row| File::try_from(row))?
            .collect()
    }

    pub fn find_identical_signs(&self) -> Result<Vec<(String, i64, i64)>, rusqlite::Error> {
        self.connection
            .prepare_cached(Self::FIND_IDENTICAL_SIGNS)?
            .query_map((), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?
            .collect()
    }

    pub fn mark_original_files(&self) -> Result<usize, rusqlite::Error> {
        self.connection
            .prepare_cached(Self::MARK_ORIGINAL_FILES)?
            .execute(())
    }

    pub fn count_original_files(&self) -> Result<i64, rusqlite::Error> {
        self.connection
            .prepare_cached("SELECT COUNT(*) AS cnt FROM files WHERE is_original = 1;")?
            .query_one((), |row| row.get::<_, i64>(0))
    }

    pub fn update_optimized_file(&self, file: &File) -> Result<(), rusqlite::Error> {
        self.connection
            .prepare_cached(Self::UPDATE_OPTIMIZED_FILE)?
            .execute((&file.optimized, file.size_bytes, &file.path))?;
        Ok(())
    }

    const UPDATE_OPTIMIZED_FILE: &'static str = r#"
        UPDATE files
        SET optimized = ?1, size_bytes = ?2
        WHERE path = ?3;
    "#;

    const MARK_ORIGINAL_FILES: &'static str = r#"
        UPDATE files
        SET is_original = CASE
            WHEN rowid = (
                SELECT rowid
                FROM files f2
                WHERE f2.blake3 = files.blake3
                  AND f2.size_bytes = files.size_bytes
                ORDER BY created_at ASC, rowid ASC
                LIMIT 1
            )
            THEN 1
            ELSE 0
        END;
    "#;

    pub const FIND_UNIQUE_FILES_ORDERED: &'static str = r#"
        SELECT * FROM (
        	SELECT *, ROW_NUMBER() OVER (PARTITION BY blake3, size_bytes)
        	AS rn
        	FROM files
        	ORDER BY created_at ASC
        ) ranked
        WHERE rn = 1;
    "#;

    pub const FIND_UNOPTIMIZED_VIDEOS: &'static str = r#"
        SELECT * FROM files WHERE media_type = 'video' AND is_original = 1 AND optimized IS NULL;
    "#;

    pub const FIND_UNIQUE_FILES: &'static str = r#"
        SELECT * FROM (
        	SELECT *, ROW_NUMBER() OVER (PARTITION BY blake3, size_bytes)
        	AS rn
        	FROM files
        ) ranked
        WHERE rn = 1;
    "#;

    const COUNT_FILES: &'static str = r#"
        SELECT COUNT(path) AS cnt
        FROM files;
    "#;

    const FIND_DUP_FILES: &'static str = r#"
        SELECT path, size_bytes, blake3, created_at
        FROM files
        WHERE blake3 = ?1
          AND size_bytes = ?2;
    "#;

    const FIND_IDENTICAL_SIGNS: &'static str = r#"
        SELECT blake3, size_bytes, COUNT(*) AS cnt
        FROM files
        GROUP BY blake3, size_bytes
        HAVING cnt > 1;
    "#;

    const INSERT_FILE: &'static str = r#"
        INSERT OR REPLACE INTO files (
            path,
            size_bytes,
            blake3,
            created_at,
            optimized,
            is_original,
            media_type
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);
    "#;

    const COUNT_REDUNDANT_FILES: &'static str = r#"
        SELECT SUM(cnt - 1) AS redundant_files
        FROM (
            SELECT COUNT(*) AS cnt
            FROM files
            GROUP BY blake3, size_bytes
            HAVING cnt > 1
        );
    "#;
}
