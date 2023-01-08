use crate::documents::EpointInfoDocument;
use crate::error::Error;
use crate::write_impl::{write_to_parquet, write_to_xyz};
use epoint_core::PointCloud;
use std::fs;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

/// `EpointWriter` sets up a writer for the custom reader data structure.
///
#[derive(Debug, Clone)]
pub struct EpointWriter {
    path: PathBuf,
    compressed: bool,
}

impl EpointWriter {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
            compressed: true,
        }
    }

    pub fn with_compressed(mut self, compressed: bool) -> Self {
        self.compressed = compressed;
        self
    }

    pub fn finish(&self, point_cloud: &PointCloud) -> Result<(), Error> {
        fs::create_dir_all(self.path.clone())?;

        if self.compressed {
            let parquet_file_path = self.path.join("point_data.parquet");
            write_to_parquet(point_cloud, &parquet_file_path)?;
        } else {
            let xyz_file_path = self.path.join("point_data.xyz");
            write_to_xyz(point_cloud, &xyz_file_path)?;
        }

        let info_document_path = self.path.join("info.json");
        let info_document =
            EpointInfoDocument::new().with_frame_id(point_cloud.info().frame_id.clone());
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(info_document_path)?;
        serde_json::to_writer_pretty(file, &info_document)?;

        let frames_document_path = self.path.join("frames.json");
        ecoord::io::EcoordWriter::new(frames_document_path)
            .finish(point_cloud.reference_frames())?;

        Ok(())
    }
}
