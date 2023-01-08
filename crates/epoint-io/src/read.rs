use crate::documents::EpointInfoDocument;
use crate::error::Error;
use crate::read_impl::read_data_frame_from_xyz_file;
use epoint_core::PointCloud;
use epoint_core::PointCloudInfo;
use std::fs::File;
use std::path::{Path, PathBuf};

/// `EpointReader` sets up a reader for the custom reader data structure.
///
#[derive(Debug, Clone)]
pub struct EpointReader {
    path: PathBuf,
}

impl EpointReader {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
        }
    }

    pub fn finish(self) -> Result<PointCloud, Error> {
        assert!(self.path.is_dir());

        let xyz_file_path = self.path.join("point_data.xyz");
        let point_data = read_data_frame_from_xyz_file(&xyz_file_path)?;

        let info_document_path = self.path.join("info.json");
        let info_file = File::open(info_document_path)?;
        let eframe_document: EpointInfoDocument =
            serde_json::from_reader(&info_file).expect("Unable to parse document");
        let info = PointCloudInfo {
            frame_id: eframe_document.frame_id.map(|f| f.into()),
        };

        let frames_document_path = self.path.join("frames.json");
        let reference_frames = ecoord::io::EcoordReader::new(frames_document_path).finish()?;

        let point_cloud = PointCloud::from_data_frame(point_data, info, reference_frames)?;
        Ok(point_cloud)
    }
}
