use crate::Error::{InvalidFileExtension, NoFileName};
use crate::epoint::write_impl::write_epoint_format;
use crate::epoint::{FILE_EXTENSION_EPOINT_FORMAT, FILE_EXTENSION_EPOINT_TAR_FORMAT};
use crate::error::Error;
use chrono::{DateTime, Utc};
use epoint_core::PointCloud;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

pub const DEFAULT_COMPRESSION_LEVEL: i32 = 10;

/// `EpointWriter` sets up a writer for the custom reader data structure.
///
#[derive(Debug, Clone)]
pub struct EpointWriter<W: Write> {
    writer: W,
    compression_level: Option<i32>,
    time: Option<DateTime<Utc>>,
}

impl<W: Write> EpointWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            compression_level: Some(DEFAULT_COMPRESSION_LEVEL),
            time: None,
        }
    }

    pub fn with_compressed(mut self, compressed: bool) -> Self {
        if compressed {
            self.compression_level = Some(DEFAULT_COMPRESSION_LEVEL);
        } else {
            self.compression_level = None;
        }
        self
    }

    pub fn with_time(mut self, time: Option<DateTime<Utc>>) -> Self {
        self.time = time;
        self
    }

    pub fn finish(self, point_cloud: PointCloud) -> Result<(), Error> {
        write_epoint_format(self.writer, point_cloud, self.compression_level, self.time)?;

        Ok(())
    }
}

impl EpointWriter<File> {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file_name_str = path
            .as_ref()
            .file_name()
            .ok_or(NoFileName())?
            .to_string_lossy()
            .to_lowercase();
        if !file_name_str.ends_with(FILE_EXTENSION_EPOINT_TAR_FORMAT)
            && !file_name_str.ends_with(FILE_EXTENSION_EPOINT_FORMAT)
        {
            return Err(InvalidFileExtension(file_name_str.to_string()));
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(Self::new(file))
    }
}
