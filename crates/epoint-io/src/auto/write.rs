use crate::Error::{FormatNotSupported, InvalidFileExtension};
use crate::format::PointCloudFormat;
use crate::{EpointWriter, Error, LasWriter, XyzWriter};
use epoint_core::PointCloud;
use std::path::{Path, PathBuf};

/// `AutoWriter` sets up a writer that automatically determines format and writes the point cloud
/// file.
///
#[derive(Debug, Clone)]
pub struct AutoWriter {
    path: PathBuf,
    format: PointCloudFormat,
}

impl AutoWriter {
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

    pub fn from_base_path_with_format(
        base_path: impl AsRef<Path>,
        format: PointCloudFormat,
    ) -> Result<Self, Error> {
        let base_path = base_path.as_ref();
        let extension = format.extension();

        // Convert the path to a string and append the extension
        let mut path_string = base_path.to_string_lossy().into_owned();
        path_string.push('.');
        path_string.push_str(extension);

        Ok(Self {
            path: PathBuf::from(path_string),
            format,
        })
    }

    pub fn finish(self, point_cloud: PointCloud) -> Result<(), Error> {
        match self.format {
            PointCloudFormat::Epoint => EpointWriter::from_path(self.path)?.finish(point_cloud),
            PointCloudFormat::EpointTar => EpointWriter::from_path(self.path)?
                .with_compressed(false)
                .finish(point_cloud),
            PointCloudFormat::E57 => Err(FormatNotSupported(
                "E57 not supported for reading".to_string(),
            )),
            PointCloudFormat::Las => LasWriter::from_path(self.path)?.finish(point_cloud),
            PointCloudFormat::Laz => LasWriter::from_path(self.path)?.finish(point_cloud),
            PointCloudFormat::Xyz => XyzWriter::from_path(self.path)?
                .with_compressed(false)
                .finish(point_cloud),
            PointCloudFormat::XyzZst => XyzWriter::from_path(self.path)?.finish(point_cloud),
        }
    }
}
