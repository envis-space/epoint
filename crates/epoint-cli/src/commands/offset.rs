use crate::error::Error;
use epoint::io::LasWriter;
use nalgebra::Vector3;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::info;

pub fn run(
    input_directory: impl AsRef<Path>,
    output_directory: impl AsRef<Path>,
    translation_offset: Vector3<f64>,
) -> Result<(), Error> {
    info!(
        "Start translating with {}, {}, {}",
        translation_offset[0], translation_offset[1], translation_offset[2]
    );

    let paths = fs::read_dir(input_directory)?;
    let output_directory = PathBuf::from(output_directory.as_ref());
    fs::create_dir_all(&output_directory)?;

    for current_dir_entry in paths {
        info!(
            "Processing: {}",
            current_dir_entry.as_ref().unwrap().path().display()
        );
        let current_path = current_dir_entry?.path();

        let (point_cloud, _las_read_info) = epoint::io::LasReader::from_path(&current_path)?
            .normalize_colors(true)
            .finish()?;

        let translated_point_cloud =
            epoint::transform::translate(&point_cloud, translation_offset)?;

        let file_name = current_path.file_name().unwrap();
        LasWriter::from_path(output_directory.join(file_name))?.finish(translated_point_cloud)?;
    }

    Ok(())
}
