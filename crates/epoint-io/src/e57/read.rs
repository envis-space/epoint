use crate::Error;
use crate::Error::{InvalidFileExtension, NoFileExtension};
use crate::e57::FILE_EXTENSION_E57_FORMAT;
use crate::e57::read_impl::import_point_cloud_from_e57_file;
use chrono::Utc;
use ecoord::{ChannelId, FrameId};
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
    acquisition_start_timestamps: Option<Vec<chrono::DateTime<Utc>>>,
    channel_id: ChannelId,
    world_frame_id: FrameId,
    scanner_frame_id: FrameId,
}

impl<R: Read + Seek> E57Reader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            acquisition_start_timestamps: None,
            channel_id: "default".into(),
            world_frame_id: "world".into(),
            scanner_frame_id: "scanner".into(),
        }
    }

    pub fn with_acquisition_start_timestamps(
        mut self,
        acquisition_start_timestamps: Vec<chrono::DateTime<Utc>>,
    ) -> Self {
        self.acquisition_start_timestamps = Some(acquisition_start_timestamps);
        self
    }

    pub fn finish(self) -> Result<PointCloud, Error> {
        let point_cloud = import_point_cloud_from_e57_file(
            self.reader,
            self.acquisition_start_timestamps,
            self.channel_id,
            self.world_frame_id,
            self.scanner_frame_id,
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
