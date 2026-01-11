use std::{
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

use chrono::{DateTime, Local, TimeZone};

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
            created_at  INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_files_blake3 ON files (blake3);
        CREATE INDEX IF NOT EXISTS idx_files_created ON files (created_at);
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
            .query_map((blake3, size_bytes), |row| {
                let ts: i64 = row.get(3)?;
                Ok(File {
                    path: row.get(0)?,
                    size_bytes: row.get(1)?,
                    blake3: row.get(2)?,
                    created_at: Local.timestamp_opt(ts, 0).single().unwrap(),
                })
            })?
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

    pub const FIND_UNIQUE_FILES_ORDERED: &'static str = r#"
        SELECT * FROM (
        	SELECT *, ROW_NUMBER() OVER (PARTITION BY blake3, size_bytes)
        	AS rn
        	FROM files
        	ORDER BY created_at ASC
        ) ranked
        WHERE rn = 1;
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
            created_at
        ) VALUES (?1, ?2, ?3, ?4);
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
