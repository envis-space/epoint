use crate::las::read_impl::import_point_cloud_from_las_reader;
use crate::{Error, FILE_EXTENSION_LAS_FORMAT, FILE_EXTENSION_LAZ_FORMAT};

use crate::las::LasVersion;
use epoint_core::PointCloud;

use crate::Error::{InvalidFileExtension, NoFileExtension};
use ecoord::FrameId;
use ecoord::io::EcoordReader;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, Seek};
use std::path::Path;

/// `LasReader` imports a point cloud from a LAS or LAZ file.
///
#[derive(Debug, Clone)]
pub struct LasReader<R: Read + Seek + Send + Sync + Debug> {
    reader: R,
    sidecar_ecoord_reader: Option<EcoordReader<R>>,
    normalize_colors: bool,
    reference_frame_id: FrameId,
    points_per_chunk: Option<u64>,
}

impl<R: Read + Seek + Send + Sync + 'static + Debug> LasReader<R> {
    /// Create a new [`LasReader`] from an existing `Reader`.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            sidecar_ecoord_reader: None,
            normalize_colors: false,
            reference_frame_id: FrameId::global(),
            points_per_chunk: Some(100_000_000),
        }
    }

    pub fn normalize_colors(mut self, normalize_colors: bool) -> Self {
        self.normalize_colors = normalize_colors;
        self
    }

    pub fn with_sidecar_ecoord_reader(mut self, reader: Option<EcoordReader<R>>) -> Self {
        self.sidecar_ecoord_reader = reader;
        self
    }

    pub fn with_points_per_chunk(mut self, points_per_chunk: Option<u64>) -> Self {
        self.points_per_chunk = points_per_chunk;
        self
    }

    pub fn finish(self) -> Result<(PointCloud, LasReadInfo), Error> {
        let (mut point_cloud, read_info) = import_point_cloud_from_las_reader(
            self.reader,
            self.normalize_colors,
            self.reference_frame_id,
            self.points_per_chunk,
        )?;

        if let Some(reader) = self.sidecar_ecoord_reader {
            let transform_tree = reader.finish()?;
            point_cloud.append_transform_tree(transform_tree)?;
        }

        Ok((point_cloud, read_info))
    }
}

impl LasReader<File> {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, Error> {
        let extension = path.as_ref().extension().ok_or(NoFileExtension())?;
        if extension != FILE_EXTENSION_LAS_FORMAT && extension != FILE_EXTENSION_LAZ_FORMAT {
            return Err(InvalidFileExtension(
                extension.to_str().unwrap_or_default().to_string(),
            ));
        }

        // read sidecar ecoord file if available
        let sidecar_ecoord_reader = EcoordReader::from_base_path(
            path.as_ref().parent().unwrap(),
            path.as_ref().file_stem().unwrap().to_str().unwrap(),
        )?;

        let file = File::open(path)?;
        let las_reader = Self::new(file).with_sidecar_ecoord_reader(sidecar_ecoord_reader);

        Ok(las_reader)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LasReadInfo {
    pub version: LasVersion,
}
