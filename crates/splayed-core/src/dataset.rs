//! Dataset: a directory containing `.meta` + FIELD files.
//!
//! See `plan.md` §4.2 for partition layout.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use splayed_format::{MetaFile, META_FILE_NAME};

/// An open dataset: a directory with a `.meta` file and zero or more FIELD files.
#[derive(Debug)]
pub struct Dataset {
    pub dir: PathBuf,
    pub meta: MetaFile,
}

impl Dataset {
    /// Path to the `.meta` file.
    pub fn meta_path(&self) -> PathBuf {
        self.dir.join(META_FILE_NAME)
    }

    /// Path to a named FIELD file (e.g. "close").
    pub fn field_path(&self, field_name: &str) -> PathBuf {
        self.dir.join(field_name)
    }

    /// List the FIELD files present in this dataset (excluding `.meta`).
    pub fn list_fields(&self) -> std::io::Result<Vec<String>> {
        let mut fields = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name != META_FILE_NAME && entry.file_type()?.is_file() {
                fields.push(name);
            }
        }
        fields.sort();
        Ok(fields)
    }

    /// Check which of the given field names exist on disk.
    pub fn existing_fields(&self, names: &[String]) -> std::io::Result<HashSet<String>> {
        let present: HashSet<String> = self.list_fields()?.into_iter().collect();
        Ok(names.iter().filter(|n| present.contains(*n)).cloned().collect())
    }
}

/// Open a dataset directory: read and parse the `.meta` file.
pub fn open_dataset(dir: impl AsRef<Path>) -> Result<Dataset, DatasetError> {
    let dir = dir.as_ref().to_path_buf();
    let meta_path = dir.join(META_FILE_NAME);
    let bytes = fs::read(&meta_path).map_err(DatasetError::Io)?;
    let meta = MetaFile::deserialize(&bytes).map_err(DatasetError::Meta)?;
    Ok(Dataset { dir, meta })
}

#[derive(Debug)]
pub enum DatasetError {
    Io(std::io::Error),
    Meta(splayed_format::MetaError),
}

impl std::fmt::Display for DatasetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "dataset io error: {e}"),
            Self::Meta(e) => write!(f, "dataset meta error: {e}"),
        }
    }
}

impl std::error::Error for DatasetError {}
