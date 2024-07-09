use crate::las::read_impl::import_point_cloud_from_las_file;
use crate::{Error, FILE_EXTENSION_LAS_FORMAT, FILE_EXTENSION_LAZ_FORMAT};

use crate::las::LasVersion;
use epoint_core::PointCloud;

use crate::Error::{InvalidFileExtension, NoFileExtension};
use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, Seek};
use std::path::Path;

/// `LasReader` imports a point cloud from a LAS or LAZ file.
///
#[derive(Debug, Clone)]
pub struct LasReader<R: Read + Seek + Send + Debug> {
    reader: R,
    normalize_colors: bool,
}

impl<R: Read + Seek + Send + Debug> LasReader<R> {
    /// Create a new [`LasReader`] from an existing `Reader`.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            normalize_colors: false,
        }
    }

    pub fn normalize_colors(mut self, normalize_colors: bool) -> Self {
        self.normalize_colors = normalize_colors;
        self
    }

    pub fn finish(self) -> Result<(PointCloud, LasReadInfo), Error> {
        let point_cloud = import_point_cloud_from_las_file(self.reader, self.normalize_colors)?;
        Ok(point_cloud)
    }
}

impl LasReader<File> {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, Error> {
        let extension = path.as_ref().extension().ok_or(NoFileExtension())?;
        if extension != FILE_EXTENSION_LAS_FORMAT && extension != FILE_EXTENSION_LAZ_FORMAT {
            return Err(InvalidFileExtension(
                extension.to_str().unwrap_or_default().to_string(),
            ));
        }

        let file = File::open(path)?;
        Ok(Self::new(file))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LasReadInfo {
    pub version: LasVersion,
}
