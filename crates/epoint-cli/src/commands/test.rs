use epoint::io::EpointWriter;
use std::path::Path;
use std::time::Instant;
use tracing::info;

pub fn run(input_path: impl AsRef<Path>, output_directory_path: impl AsRef<Path>) {
    let start = Instant::now();
    /*let point_cloud = EpointReader::from_path(input_path)
    .unwrap()
    .finish()
    .unwrap();*/
    let (point_cloud, _las_read_info) = epoint::io::LasReader::from_path(&input_path)
        .unwrap()
        .normalize_colors(true)
        .finish()
        .unwrap();
    /*let mut point_cloud = epoint::io::XyzReader::new(&input_path)
    .with_separator(b',')
    .finish()
    .unwrap();*/
    let duration = start.elapsed();
    info!(
        "Imported point cloud with {} points in {:?}.",
        point_cloud.size(),
        duration
    );

    /*let octree = PointCloudOctree::from_point_cloud(point_cloud, 100000, Some(123)).unwrap();
    info!("Octree {:?}", octree);
    info!("Octree length {:?}", octree.number_of_cells());*/

    info!(
        "Start writing point cloud to {}",
        output_directory_path.as_ref().display()
    );
    EpointWriter::from_path(output_directory_path)
        .unwrap()
        .with_compressed(false)
        .finish(point_cloud)
        .expect("should work");
    /*LasWriter::from_path(output_directory_path)
    .unwrap()
    .finish(&point_cloud)
    .expect("should work");*/
}
