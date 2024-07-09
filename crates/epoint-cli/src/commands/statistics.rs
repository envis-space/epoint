use std::path::Path;
use std::time::Instant;
use tracing::info;

pub fn run(file_path: impl AsRef<Path>) {
    info!("Start statistics");

    let now = Instant::now();
    /*let (point_cloud, _las_read_info) = epoint::io::LasReader::from_path(file_path)
    .unwrap()
    .finish()
    .unwrap();*/
    let point_cloud = epoint::io::EpointReader::from_path(file_path)
        .unwrap()
        .finish()
        .unwrap();
    info!("Read point cloud in {}s", now.elapsed().as_secs());

    info!("Number of points: {}\n", point_cloud.size());
}
