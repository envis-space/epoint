use crate::epoint::documents::EpointInfoDocument;
use crate::epoint::{
    EPOINT_SEPARATOR, FILE_NAME_ECOORD_COMPRESSED, FILE_NAME_ECOORD_UNCOMPRESSED,
    FILE_NAME_INFO_COMPRESSED, FILE_NAME_INFO_UNCOMPRESSED, FILE_NAME_POINT_DATA_COMPRESSED,
    FILE_NAME_POINT_DATA_UNCOMPRESSED,
};
use crate::error::Error;
use chrono::{DateTime, Utc};
use epoint_core::PointCloud;
use polars::prelude::{CsvWriter, ParquetWriter, SerWriter, StatisticsOptions};
use std::io::{Cursor, Write};
use tar::Builder;

pub fn write_epoint_format<W: Write>(
    writer: W,
    mut point_cloud: PointCloud,
    compression_level: Option<i32>,
    time: Option<DateTime<Utc>>,
) -> Result<(), Error> {
    let mut archive_builder = Builder::new(writer);

    // info document
    let info_document =
        EpointInfoDocument::new().with_frame_id(point_cloud.info().frame_id.clone());
    let mut info_document_buffer: Vec<u8> = Vec::new();
    if let Some(compression_level) = compression_level {
        serde_json::to_writer(&mut info_document_buffer, &info_document)?;
        let mut info_document_compressed_buffer: Vec<u8> = Vec::new();
        zstd::stream::copy_encode(
            Cursor::new(info_document_buffer),
            &mut info_document_compressed_buffer,
            compression_level,
        )?;
        archive_builder.append_data(
            &mut create_archive_header(info_document_compressed_buffer.len(), time),
            FILE_NAME_INFO_COMPRESSED,
            Cursor::new(info_document_compressed_buffer),
        )?;
    } else {
        serde_json::to_writer_pretty(&mut info_document_buffer, &info_document)?;
        archive_builder.append_data(
            &mut create_archive_header(info_document_buffer.len(), time),
            FILE_NAME_INFO_UNCOMPRESSED,
            Cursor::new(info_document_buffer),
        )?;
    }

    // ecoord document
    let mut ecoord_document_buffer: Vec<u8> = Vec::new();
    ecoord::io::EcoordWriter::new(&mut ecoord_document_buffer)
        .with_pretty_write(compression_level.is_none())
        .finish(point_cloud.reference_frames())?;
    if let Some(compression_level) = compression_level {
        let mut ecoord_document_compressed_buffer: Vec<u8> = Vec::new();
        zstd::stream::copy_encode(
            Cursor::new(ecoord_document_buffer),
            &mut ecoord_document_compressed_buffer,
            compression_level,
        )?;
        archive_builder.append_data(
            &mut create_archive_header(ecoord_document_compressed_buffer.len(), time),
            FILE_NAME_ECOORD_COMPRESSED,
            Cursor::new(ecoord_document_compressed_buffer),
        )?;
    } else {
        archive_builder.append_data(
            &mut create_archive_header(ecoord_document_buffer.len(), time),
            FILE_NAME_ECOORD_UNCOMPRESSED,
            Cursor::new(ecoord_document_buffer),
        )?;
    }

    // point data
    let mut point_data_buffer: Vec<u8> = Vec::new();
    if compression_level.is_some() {
        ParquetWriter::new(&mut point_data_buffer)
            .with_statistics(StatisticsOptions::default())
            .finish(&mut point_cloud.point_data.data_frame)?;
        archive_builder.append_data(
            &mut create_archive_header(point_data_buffer.len(), time),
            FILE_NAME_POINT_DATA_COMPRESSED,
            Cursor::new(point_data_buffer),
        )?;
    } else {
        CsvWriter::new(&mut point_data_buffer)
            .with_separator(EPOINT_SEPARATOR)
            .finish(&mut point_cloud.point_data.data_frame)?;
        archive_builder.append_data(
            &mut create_archive_header(point_data_buffer.len(), time),
            FILE_NAME_POINT_DATA_UNCOMPRESSED,
            Cursor::new(point_data_buffer),
        )?;
    }

    Ok(())
}

fn create_archive_header(size: usize, time: Option<chrono::DateTime<Utc>>) -> tar::Header {
    let mut header = tar::Header::new_gnu();
    header.set_size(size as u64);
    header.set_mode(0o664);
    if let Some(time) = time {
        header.set_mtime(time.timestamp() as u64);
    }
    header.set_cksum();

    header
}
