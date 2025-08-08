use crate::Error::{FormatNotSupported, InvalidFileExtension};
use crate::format::PointCloudFormat;
use crate::{E57Reader, EpointReader, Error, LasReader, XyzReader};
use epoint_core::PointCloud;
use std::path::{Path, PathBuf};

/// `AutoReader` sets up a reader that automatically determines format and reads the point cloud
/// file.
///
#[derive(Debug, Clone)]
pub struct AutoReader {
    path: PathBuf,
    format: PointCloudFormat,
}

impl AutoReader {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file_path = path.as_ref().to_path_buf();

        let format = PointCloudFormat::from_path(&file_path).ok_or(InvalidFileExtension(
            file_path
                .extension()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        ))?;

        Ok(Self {
            path: file_path,
            format,
        })
    }

    pub fn finish(&self) -> Result<PointCloud, Error> {
        match self.format {
            PointCloudFormat::Epoint => EpointReader::from_path(&self.path)?.finish(),
            PointCloudFormat::EpointTar => EpointReader::from_path(&self.path)?.finish(),
            PointCloudFormat::E57 => E57Reader::from_path(&self.path)?.finish(),
            PointCloudFormat::Las => Ok(LasReader::from_path(&self.path)?.finish()?.0),
            PointCloudFormat::Laz => Ok(LasReader::from_path(&self.path)?.finish()?.0),
            PointCloudFormat::Xyz => XyzReader::from_path(&self.path)?.finish(),
            PointCloudFormat::XyzZst => Err(FormatNotSupported(
                "XyzZst not supported for writing".to_string(),
            )),
        }
    }
}
