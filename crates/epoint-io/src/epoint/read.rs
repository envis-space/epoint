use crate::Error::{FileNotFound, InvalidFileExtension, NoFileName};
use crate::epoint::documents::EpointInfoDocument;
use crate::epoint::read_impl::cast_data_frame;
use crate::epoint::{
    EPOINT_SEPARATOR, FILE_EXTENSION_EPOINT_FORMAT, FILE_EXTENSION_EPOINT_TAR_FORMAT,
    FILE_NAME_ECOORD_COMPRESSED, FILE_NAME_ECOORD_UNCOMPRESSED, FILE_NAME_INFO_COMPRESSED,
    FILE_NAME_INFO_UNCOMPRESSED, FILE_NAME_POINT_DATA_COMPRESSED,
    FILE_NAME_POINT_DATA_UNCOMPRESSED,
};
use crate::error::Error;
use ecoord::ReferenceFrames;
use epoint_core::PointCloud;
use epoint_core::PointCloudInfo;
use polars::prelude::*;
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;
use tar::Archive;

/// `EpointReader` sets up a reader for the custom reader data structure.
///
#[derive(Debug, Clone)]
pub struct EpointReader<R: Read> {
    reader: R,
}

impl<R: Read> EpointReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    pub fn finish(self) -> Result<PointCloud, Error> {
        let mut archive = Archive::new(self.reader);

        let mut info_document: Option<EpointInfoDocument> = None;
        let mut point_data_frame: Option<DataFrame> = None;
        let mut reference_frames: Option<ReferenceFrames> = None;

        for file in archive.entries()? {
            let mut f = file?;

            match f.path()?.to_str().unwrap() {
                FILE_NAME_INFO_UNCOMPRESSED => {
                    info_document = serde_json::from_reader(f)?;
                }
                FILE_NAME_INFO_COMPRESSED => {
                    let mut decompressed_buffer: Vec<u8> = Vec::new();
                    zstd::stream::copy_decode(f, &mut decompressed_buffer)?;
                    info_document = serde_json::from_reader(Cursor::new(decompressed_buffer))?;
                }
                FILE_NAME_POINT_DATA_UNCOMPRESSED => {
                    let mut buffer: Vec<u8> = Vec::new();
                    f.read_to_end(&mut buffer)?;
                    let reader = Cursor::new(&buffer);

                    let csv_parse_options =
                        CsvParseOptions::default().with_separator(EPOINT_SEPARATOR);
                    let data_frame: DataFrame = CsvReadOptions::default()
                        .with_parse_options(csv_parse_options)
                        .into_reader_with_file_handle(reader)
                        .finish()?;
                    let casted_data_frame = cast_data_frame(data_frame)?;

                    point_data_frame = Some(casted_data_frame);
                }
                FILE_NAME_POINT_DATA_COMPRESSED => {
                    let mut buffer: Vec<u8> = Vec::new();
                    f.read_to_end(&mut buffer)?;
                    let reader = Cursor::new(&buffer);

                    let data_frame: DataFrame = ParquetReader::new(reader).finish()?;
                    let casted_data_frame = cast_data_frame(data_frame)?;

                    point_data_frame = Some(casted_data_frame);
                }
                FILE_NAME_ECOORD_UNCOMPRESSED => {
                    reference_frames = Some(ecoord::io::EcoordReader::new(f).finish()?);
                }
                FILE_NAME_ECOORD_COMPRESSED => {
                    let mut decompressed_buffer: Vec<u8> = Vec::new();
                    zstd::stream::copy_decode(f, &mut decompressed_buffer)?;
                    reference_frames = Some(
                        ecoord::io::EcoordReader::new(Cursor::new(decompressed_buffer)).finish()?,
                    );
                }
                _ => {}
            }
        }

        let info: PointCloudInfo = info_document
            .ok_or(FileNotFound("info".to_string()))?
            .into();
        let point_data_frame = point_data_frame.ok_or(FileNotFound("point_data".to_string()))?;
        let reference_frames = reference_frames.ok_or(FileNotFound("ecoord".to_string()))?;

        let point_cloud = PointCloud::from_data_frame(point_data_frame, info, reference_frames)?;
        Ok(point_cloud)
    }
}

impl EpointReader<File> {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file_name_str = path
            .as_ref()
            .file_name()
            .ok_or(NoFileName())?
            .to_string_lossy()
            .to_lowercase();
        if !file_name_str.ends_with(FILE_EXTENSION_EPOINT_TAR_FORMAT)
            && !file_name_str.ends_with(FILE_EXTENSION_EPOINT_FORMAT)
        {
            return Err(InvalidFileExtension(file_name_str.to_string()));
        }

        let file = std::fs::File::open(path)?;
        Ok(Self::new(file))
    }
}
