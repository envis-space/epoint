use crate::error::Error;
use epoint::PointCloud;
use epoint::io::{ColorDepth, FILE_EXTENSION_XYZ_FORMAT, XyzReader, XyzWriter};
use epoint::transform::merge;
use std::path::{Path, PathBuf};
use tracing::info;
use walkdir::WalkDir;

pub fn run(input_directory: impl AsRef<Path>, output_file: impl AsRef<Path>) -> Result<(), Error> {
    info!("Merge");

    let file_paths: Vec<PathBuf> = WalkDir::new(input_directory)
        .sort_by_file_name()
        .into_iter()
        .filter(|r| r.is_ok())
        .map(|r| r.unwrap().path().to_owned())
        .filter(|x| {
            x.extension()
                .is_some_and(|ext| ext == FILE_EXTENSION_XYZ_FORMAT)
        })
        .collect();
    info!("Total {}", file_paths.len());

    let point_clouds: Vec<PointCloud> = file_paths
        .iter()
        .enumerate()
        .map(|(current_index, current_path)| {
            info!("Read {}/{}", current_index, file_paths.len());

            /*let filtered_df = point_cloud
                .point_data
                .data_frame
                .clone()
                .lazy()
                .filter(col("gml_id").neq(lit("")))
                .collect()?;
            point_cloud.point_data.data_frame = filtered_df;*/

            XyzReader::from_path(current_path)?.finish()
        })
        .collect::<Result<Vec<_>, _>>()?;

    info!("Start merging point clouds");
    let merged_point_cloud = merge(point_clouds)?;

    info!("Start writing");
    XyzWriter::from_path(output_file.as_ref())?
        .with_color_depth(ColorDepth::EightBit)
        .finish(merged_point_cloud)?;

    Ok(())
}
