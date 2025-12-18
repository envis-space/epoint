use crate::las::write_impl::write_las_format;
use crate::{Error, FILE_EXTENSION_LAS_FORMAT, FILE_EXTENSION_LAZ_FORMAT};
use epoint_core::PointCloud;

use crate::Error::{InvalidFileExtension, NoFileExtension};
use ecoord::FrameId;
use std::fmt::Debug;
use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::path::Path;

/// `EpointWriter` sets up a writer for the custom reader data structure.
///
#[derive(Debug, Clone)]
pub struct LasWriter<W: 'static + Write + Seek + Sync + Debug + Send> {
    writer: W,
    frame_id: Option<FrameId>,
}

impl<W: Write + Seek + Sync + Debug + Send> LasWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            frame_id: None,
        }
    }

    pub fn with_frame_id(mut self, frame_id: FrameId) -> Self {
        self.frame_id = Some(frame_id);
        self
    }

    pub fn finish(self, mut point_cloud: PointCloud) -> Result<(), Error> {
        if let Some(frame_id) = self.frame_id {
            point_cloud.resolve_to_frame(frame_id)?;
        };

        write_las_format(BufWriter::new(self.writer), point_cloud)?;

        Ok(())
    }
}

impl LasWriter<File> {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, Error> {
        let extension = path.as_ref().extension().ok_or(NoFileExtension())?;
        if extension != FILE_EXTENSION_LAS_FORMAT && extension != FILE_EXTENSION_LAZ_FORMAT {
            return Err(InvalidFileExtension(
                extension.to_str().unwrap_or_default().to_string(),
            ));
        }

        let file = File::create(path)?;
        Ok(Self::new(file))
    }
}
