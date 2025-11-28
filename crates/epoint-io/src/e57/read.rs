use crate::Error;
use crate::Error::{InvalidFileExtension, NoFileExtension};
use crate::e57::FILE_EXTENSION_E57_FORMAT;
use crate::e57::read_impl::import_point_cloud_from_e57_file;
use ecoord::FrameId;
use epoint_core::PointCloud;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, Seek};
use std::path::Path;

/// `E57Reader` imports a point cloud from a E57 file.
///
#[derive(Debug, Clone)]
pub struct E57Reader<R: Read + Seek> {
    reader: R,
    reference_frame_id: FrameId,
    sensor_frame_id: FrameId,
}

impl<R: Read + Seek> E57Reader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            reference_frame_id: FrameId::global(),
            sensor_frame_id: FrameId::sensor(),
        }
    }

    pub fn finish(self) -> Result<PointCloud, Error> {
        let point_cloud = import_point_cloud_from_e57_file(
            self.reader,
            self.reference_frame_id,
            self.sensor_frame_id,
        )?;

        Ok(point_cloud)
    }
}

impl E57Reader<File> {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, Error> {
        let extension = path.as_ref().extension().ok_or(NoFileExtension())?;
        if extension != FILE_EXTENSION_E57_FORMAT {
            return Err(InvalidFileExtension(
                extension.to_str().unwrap_or_default().to_string(),
            ));
        }

        let file = File::open(path)?;
        Ok(Self::new(file))
    }
}
