use epoint::io::EpointWriter;

use std::path::Path;
use std::time::Instant;
use tracing::info;

pub fn run(input_directory_path: impl AsRef<Path>, output_directory_path: impl AsRef<Path>) {
    let start = Instant::now();
    let mut point_cloud = epoint::io::EpointReader::new(input_directory_path)
        .finish()
        .unwrap();
    let duration = start.elapsed();
    info!(
        "Read point cloud with {} points in {:?}.",
        point_cloud.size(),
        duration
    );

    // "slam_map"
    point_cloud.resolve_to_frame("slam_map".into()).unwrap();

    info!(
        "Start writing point cloud to {}",
        output_directory_path.as_ref().display()
    );
    EpointWriter::new(output_directory_path)
        .with_compressed(false)
        .finish(&point_cloud)
        .expect("should work");
}
