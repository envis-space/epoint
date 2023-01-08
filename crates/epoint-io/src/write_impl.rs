use crate::error::Error;
use epoint_core::PointCloud;
use polars::prelude::{CsvWriter, ParquetWriter, SerWriter};
use std::fs::OpenOptions;
use std::path::Path;

pub fn write_to_xyz(point_cloud: &PointCloud, file_path: impl AsRef<Path>) -> Result<(), Error> {
    // fs::remove_file(file_path).expect("File delete failed");

    let mut point_data = point_cloud.point_data().clone();

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_path)?;
    CsvWriter::new(file)
        .with_delimiter(b' ')
        .finish(&mut point_data)?;

    Ok(())
}

pub fn write_to_parquet(
    point_cloud: &PointCloud,
    file_path: impl AsRef<Path>,
) -> Result<(), Error> {
    let mut point_data = point_cloud.point_data().clone();

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_path)?;

    ParquetWriter::new(file)
        .with_statistics(true)
        .finish(&mut point_data)?;

    Ok(())
}
