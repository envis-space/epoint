use crate::error::Error;

use epoint_core::point_cloud::PointCloud;

use crate::xyz::read_impl::read_point_cloud_from_xyz_file;
use crate::Error::{InvalidFileExtension, NoFileExtension};
use crate::FILE_EXTENSION_XYZ_FORMAT;
use std::path::{Path, PathBuf};

/// `XyzReader` imports a point cloud from an XYZ file.
///
#[derive(Debug, Clone)]
pub struct XyzReader {
    path: PathBuf,
    separator: u8,
}

impl XyzReader {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
            separator: b' ',
        }
    }

    pub fn with_separator(mut self, separator: u8) -> Self {
        self.separator = separator;
        self
    }

    pub fn finish(self) -> Result<PointCloud, Error> {
        let extension = self.path.extension().ok_or(NoFileExtension())?;
        if extension != FILE_EXTENSION_XYZ_FORMAT {
            return Err(InvalidFileExtension(
                extension.to_str().unwrap_or_default().to_string(),
            ));
        }

        let point_cloud = read_point_cloud_from_xyz_file(&self.path, self.separator)?;
        Ok(point_cloud)
    }
}
