use crate::error::Error;
use crate::Error::{InvalidFileExtension, NoFileExtension};
use crate::FILE_EXTENSION_XYZ_FORMAT;
use ecoord::FrameId;
use epoint_core::point_cloud::PointCloud;
use epoint_core::PointDataColumnType;
use palette::Srgb;
use polars::prelude::{CsvWriter, NamedFrom, SerWriter, Series};
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use std::fs::OpenOptions;
use std::path::PathBuf;

/// `XyzWriter` exports a point cloud to a non-native representation.
///
#[derive(Debug, Clone)]
pub struct XyzWriter {
    // TODO: more abstract
    path: PathBuf,
    frame_id: Option<FrameId>,
    separator: u8,
    color_depth: ColorDepth,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Default)]
pub enum ColorDepth {
    EightBit,
    #[default]
    SixteenBit,
}

impl XyzWriter {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            frame_id: None,
            separator: b' ',
            color_depth: ColorDepth::SixteenBit,
        }
    }

    pub fn with_frame_id(mut self, frame_id: FrameId) -> Self {
        self.frame_id = Some(frame_id);
        self
    }

    pub fn with_color_depth(mut self, color_depth: ColorDepth) -> Self {
        self.color_depth = color_depth;
        self
    }

    pub fn finish(&self, point_cloud: &PointCloud) -> Result<(), Error> {
        let extension = self.path.extension().ok_or(NoFileExtension())?;
        if extension != FILE_EXTENSION_XYZ_FORMAT {
            return Err(InvalidFileExtension(
                extension.to_str().unwrap_or_default().to_string(),
            ));
        }

        let mut exported_point_cloud = point_cloud.clone();
        if let Some(frame_id) = &self.frame_id {
            exported_point_cloud
                .resolve_to_frame(frame_id.clone())
                .expect("Resolving should work");
        }
        /*let mut resulting_point_cloud: PointCloud =
        self.frame_id
            .clone()
            .map_or(point_cloud.to_owned(), |f: FrameId| {
                exported_point_cloud
                    .resolve_to_frame(f)
                    .expect("Resolving should work");
                exported_point_cloud
            });*/

        if exported_point_cloud.contains_colors() {
            match self.color_depth {
                ColorDepth::EightBit => {
                    let converted_colors: Vec<Srgb<u8>> = exported_point_cloud
                        .point_data
                        .get_all_colors()?
                        .into_par_iter()
                        .map(|x| x.into_format())
                        .collect();

                    let color_red_series = Series::new(
                        PointDataColumnType::X.as_str(),
                        converted_colors.iter().map(|c| c.red).collect::<Vec<u8>>(),
                    );
                    exported_point_cloud
                        .point_data
                        .data_frame
                        .replace(PointDataColumnType::ColorRed.as_str(), color_red_series)?;

                    let color_green_series = Series::new(
                        PointDataColumnType::Y.as_str(),
                        converted_colors
                            .iter()
                            .map(|c| c.green)
                            .collect::<Vec<u8>>(),
                    );
                    exported_point_cloud
                        .point_data
                        .data_frame
                        .replace(PointDataColumnType::ColorGreen.as_str(), color_green_series)?;

                    let color_blue_series = Series::new(
                        PointDataColumnType::Z.as_str(),
                        converted_colors.iter().map(|c| c.blue).collect::<Vec<u8>>(),
                    );
                    exported_point_cloud
                        .point_data
                        .data_frame
                        .replace(PointDataColumnType::ColorBlue.as_str(), color_blue_series)?;
                }
                ColorDepth::SixteenBit => {}
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        CsvWriter::new(file)
            .with_separator(self.separator)
            .finish(&mut exported_point_cloud.point_data.data_frame)?;

        Ok(())
    }
}
