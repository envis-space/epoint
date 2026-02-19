use crate::error::Error;
use epoint::io::{AutoReader, AutoWriter, PointCloudFormat};
use nalgebra::Vector3;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::info;

pub fn run(
    input_directory: impl AsRef<Path>,
    output_directory: impl AsRef<Path>,
    translation_vector: Vector3<f64>,
    frame_id: &ecoord::FrameId,
    point_cloud_format: PointCloudFormat,
) -> Result<(), Error> {
    info!(
        "Start translating with {}, {}, {}",
        translation_vector[0], translation_vector[1], translation_vector[2]
    );

    let paths = fs::read_dir(&input_directory)?;
    let output_directory = PathBuf::from(output_directory.as_ref());
    fs::create_dir_all(&output_directory)?;

    for current_dir_entry in paths {
        let current_path = current_dir_entry?.path();
        if PointCloudFormat::from_path(&current_path).is_none() {
            continue;
        }
        info!("Processing: {}", current_path.display());

        let point_cloud = AutoReader::from_path(&current_path)?.finish()?;
        let translated_point_cloud =
            epoint::transform::translate(&point_cloud, translation_vector, frame_id)?;

        let relative_path_without_extension: PathBuf = current_path
            .strip_prefix(&input_directory)?
            .with_extension("");
        AutoWriter::from_base_path_with_format(
            output_directory.join(relative_path_without_extension),
            point_cloud_format,
        )?
        .finish(translated_point_cloud)?;
    }

    Ok(())
}
