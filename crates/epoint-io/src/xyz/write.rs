use crate::Error::{InvalidFileExtension, NoFileName};
use crate::FILE_EXTENSION_XYZ_FORMAT;
use crate::error::Error;
use crate::xyz::{DEFAULT_XYZ_SEPARATOR, FILE_EXTENSION_XYZ_ZST_FORMAT};
use ecoord::FrameId;
use epoint_core::PointDataColumnType;
use epoint_core::point_cloud::PointCloud;
use palette::Srgb;
use polars::prelude::{CsvWriter, NamedFrom, SerWriter, Series};
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

pub const DEFAULT_COMPRESSION_LEVEL: i32 = 10;
pub const DEFAULT_NULL_VALUE: &str = "NaN";

/// `XyzWriter` exports a point cloud to a non-native representation.
///
#[derive(Debug, Clone)]
pub struct XyzWriter<W: Write> {
    writer: W,
    compression_level: Option<i32>,
    frame_id: Option<FrameId>,
    separator: u8,
    null_value: String,
    color_depth: ColorDepth,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Default)]
pub enum ColorDepth {
    #[default]
    EightBit,
    SixteenBit,
}

impl<W: Write> XyzWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            compression_level: Some(crate::epoint::write::DEFAULT_COMPRESSION_LEVEL),
            frame_id: None,
            separator: DEFAULT_XYZ_SEPARATOR,
            null_value: DEFAULT_NULL_VALUE.to_string(),
            color_depth: ColorDepth::default(),
        }
    }

    pub fn with_compressed(mut self, compressed: bool) -> Self {
        if compressed {
            self.compression_level = Some(DEFAULT_COMPRESSION_LEVEL);
        } else {
            self.compression_level = None;
        }
        self
    }

    pub fn with_frame_id(mut self, frame_id: FrameId) -> Self {
        self.frame_id = Some(frame_id);
        self
    }

    pub fn with_separator(mut self, separator: u8) -> Self {
        self.separator = separator;
        self
    }

    pub fn with_null_value(mut self, null_value: String) -> Self {
        self.null_value = null_value;
        self
    }

    pub fn with_color_depth(mut self, color_depth: ColorDepth) -> Self {
        self.color_depth = color_depth;
        self
    }

    pub fn finish(self, mut point_cloud: PointCloud) -> Result<(), Error> {
        if let Some(frame_id) = &self.frame_id {
            point_cloud.resolve_to_frame(frame_id.clone())?;
        }
        /*let mut resulting_point_cloud: PointCloud =
        self.frame_id
            .clone()
            .map_or(point_cloud.to_owned(), |f: FrameId| {
                point_cloud.resolve_to_frame(f)?;
                point_cloud
            });*/

        if point_cloud.contains_colors() {
            match self.color_depth {
                ColorDepth::EightBit => {
                    let converted_colors: Vec<Srgb<u8>> = point_cloud
                        .point_data
                        .get_all_colors()?
                        .into_par_iter()
                        .map(|x| x.into_format())
                        .collect();

                    let color_red_series = Series::new(
                        PointDataColumnType::X.into(),
                        converted_colors.iter().map(|c| c.red).collect::<Vec<u8>>(),
                    );
                    point_cloud
                        .point_data
                        .data_frame
                        .replace(PointDataColumnType::ColorRed.as_str(), color_red_series)?;

                    let color_green_series = Series::new(
                        PointDataColumnType::Y.into(),
                        converted_colors
                            .iter()
                            .map(|c| c.green)
                            .collect::<Vec<u8>>(),
                    );
                    point_cloud
                        .point_data
                        .data_frame
                        .replace(PointDataColumnType::ColorGreen.as_str(), color_green_series)?;

                    let color_blue_series = Series::new(
                        PointDataColumnType::Z.into(),
                        converted_colors.iter().map(|c| c.blue).collect::<Vec<u8>>(),
                    );
                    point_cloud
                        .point_data
                        .data_frame
                        .replace(PointDataColumnType::ColorBlue.as_str(), color_blue_series)?;
                }
                ColorDepth::SixteenBit => {}
            }
        }

        let writer: Box<dyn Write> = if let Some(compression_level) = &self.compression_level {
            let buf_writer = BufWriter::with_capacity(
                zstd::stream::Encoder::<Vec<u8>>::recommended_input_size(),
                zstd::stream::Encoder::new(self.writer, *compression_level)?.auto_finish(),
            );
            Box::new(buf_writer)
        } else {
            Box::new(self.writer)
        };

        CsvWriter::new(writer)
            .with_separator(self.separator)
            .with_null_value(self.null_value)
            .finish(&mut point_cloud.point_data.data_frame)?;

        Ok(())
    }
}

impl XyzWriter<File> {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file_name_str = path
            .as_ref()
            .file_name()
            .ok_or(NoFileName())?
            .to_string_lossy()
            .to_lowercase();
        if !file_name_str.ends_with(FILE_EXTENSION_XYZ_ZST_FORMAT)
            && !file_name_str.ends_with(FILE_EXTENSION_XYZ_FORMAT)
        {
            return Err(InvalidFileExtension(file_name_str.to_string()));
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(Self::new(file))
    }
}
