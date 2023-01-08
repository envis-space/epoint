use crate::error::Error;
use crate::read_impl::read_data_frame_from_xyz_file;
use ecoord::ReferenceFrames;
use epoint_core::point_cloud::PointCloud;
use epoint_core::PointCloudInfo;

use std::path::{Path, PathBuf};

/// `EpointImporter`s imports a point cloud from a non-native representation.
///
#[derive(Debug, Clone)]
pub struct EpointImporter {
    path: PathBuf,
}

impl EpointImporter {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
        }
    }

    pub fn finish(self) -> Result<PointCloud, Error> {
        //assert!(self.path.is_dir());
        let data_frame = match self.path.extension().unwrap().to_str().unwrap() {
            "xyz" => read_data_frame_from_xyz_file(&self.path),
            _ => panic!("Problem opening the file"),
        }?;

        let point_cloud = PointCloud::from_data_frame(
            data_frame,
            PointCloudInfo::default(),
            ReferenceFrames::default(),
        )?;
        Ok(point_cloud)
    }
}
