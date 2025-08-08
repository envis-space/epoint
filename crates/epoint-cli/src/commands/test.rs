use crate::error::Error;
use epoint::io::{ColorDepth, XyzWriter};
use std::path::Path;
use std::time::Instant;
use tracing::info;

pub fn run(
    input_path: impl AsRef<Path>,
    output_directory_path: impl AsRef<Path>,
) -> Result<(), Error> {
    let start = Instant::now();
    /*let point_cloud = EpointReader::from_path(input_path)?
    .finish()?;*/
    /*let (point_cloud, _las_read_info) = epoint::io::LasReader::from_path(&input_path)?
    .normalize_colors(true)
    .finish()?;*/
    let mut point_cloud = epoint::io::XyzReader::from_path(&input_path)?
        .with_separator(b' ')
        .finish()?;
    /*let mut point_cloud = epoint::io::E57Reader::from_path(&input_path)?
    .with_acquisition_start_timestamps(vec![Utc::now()])
    .finish()?;*/
    let duration = start.elapsed();
    info!(
        "Imported point cloud with {} points in {:?}.",
        point_cloud.size(),
        duration
    );

    /*let octree = PointCloudOctree::from_point_cloud(point_cloud, 100000, Some(123))?;
    info!("Octree {:?}", octree);
    info!("Octree length {:?}", octree.number_of_cells());*/
    point_cloud.point_data.remove_colors()?;
    //let colorized_point_cloud = colorize_by_column_hash(&point_cloud, "feature_object_id")?;
    /*colorize_by_intensity_by_color_scheme_in_place(
        &mut point_cloud,
        None,
        None,
        false,
        ColorScheme::Inferno,
        false,
    )?;*/

    // point_cloud.resolve_to_frame("world".into())?;

    info!(
        "Start writing point cloud to {}",
        output_directory_path.as_ref().display()
    );
    /*EpointWriter::from_path(output_directory_path)?
    .with_compressed(false)
    .finish(colorized_point_cloud)?;*/
    XyzWriter::from_path(output_directory_path)?
        .with_color_depth(ColorDepth::EightBit)
        .with_compressed(false)
        .with_null_value("".to_string())
        .finish(&point_cloud)?;
    /*LasWriter::from_path(output_directory_path)?
    .finish(&point_cloud)?;*/

    Ok(())
}
