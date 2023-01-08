use crate::error::Error;
use crate::write_impl::write_to_xyz;
use ecoord::FrameId;
use epoint_core::point_cloud::PointCloud;
use std::path::PathBuf;

/// `PointCloudExporter`s exports a point cloud to a non-native representation.
///
#[derive(Debug, Clone)]
pub struct EpointExporter {
    path: PathBuf,
    frame_id: Option<FrameId>,
}

impl EpointExporter {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            frame_id: None,
        }
    }

    pub fn with_frame_id(mut self, frame_id: FrameId) -> Self {
        self.frame_id = Some(frame_id);
        self
    }

    pub fn finish(&self, point_cloud: &PointCloud) -> Result<(), Error> {
        //assert!(self.format != PointCloudFormat::LAS, "LAS not supported yet.");
        //assert!(self.format != PointCloudFormat::LAZ, "LAZ not supported yet.");

        let target_frame_id = self.frame_id.clone();

        let resulting_point_cloud: PointCloud =
            target_frame_id.map_or(point_cloud.to_owned(), |f: FrameId| {
                let median_time = point_cloud.get_median_time();
                epoint_transform::transform_to_frame(point_cloud, median_time, &f)
            });

        assert_eq!(self.path.extension().unwrap(), "xyz");
        write_to_xyz(&resulting_point_cloud, &self.path)?;

        //assert!(self.path.is_dir());
        //let xyz_file_path = self.path.join("point_data.xyz");
        //let point_cloud = PointCloud::new(data_frame, meta_information, frames);

        Ok(())
    }
}
