use polars::prelude::{col, lit, IntoLazy};
use std::path::{Path, PathBuf};

use epoint::io::{ColorDepth, XyzReader, XyzWriter};
use epoint::transform::merge;
use epoint::PointCloud;
use tracing::info;
use walkdir::WalkDir;

pub fn run(input_directory: impl AsRef<Path>, output_file: impl AsRef<Path>) {
    info!("Merge");

    let file_paths: Vec<PathBuf> = WalkDir::new(input_directory)
        .sort_by_file_name()
        .into_iter()
        .filter(|r| r.is_ok())
        .map(|r| r.unwrap().path().to_owned())
        .filter(|x| x.extension().map_or(false, |ext| ext == "xyz"))
        .collect();
    info!("Total {}", file_paths.len());

    let point_clouds: Vec<PointCloud> = file_paths
        .iter()
        .enumerate()
        .map(|(current_index, current_path)| {
            info!("Read {}/{}", current_index, file_paths.len());

            let mut point_cloud = XyzReader::new(current_path).finish().unwrap();
            let filtered_df = point_cloud
                .point_data
                .data_frame
                .clone()
                .lazy()
                .filter(col("gml_id").neq(lit("")))
                .collect()
                .unwrap();
            point_cloud.point_data.data_frame = filtered_df;

            point_cloud
        })
        .collect();

    info!("Start merging point clouds");
    let merged_point_cloud = merge(point_clouds).unwrap();

    info!("Start writing");
    XyzWriter::new(output_file.as_ref().to_owned())
        .with_color_depth(ColorDepth::EightBit)
        .finish(&merged_point_cloud)
        .unwrap();
}
