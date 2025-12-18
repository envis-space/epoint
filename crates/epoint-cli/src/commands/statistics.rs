use crate::error::Error;
use std::path::Path;
use std::time::Instant;
use tracing::info;

pub fn run(file_path: impl AsRef<Path>) -> Result<(), Error> {
    info!("Start statistics");

    let now = Instant::now();
    let point_cloud = epoint::io::AutoReader::from_path(file_path)?.finish()?;
    info!("Read point cloud in {}s", now.elapsed().as_secs());
    info!("Number of points: {}\n", point_cloud.size());

    let timestamp_min = point_cloud.point_data.get_timestamp_min()?;
    info!("Timestamp min {:?}", timestamp_min);
    let timestamp_max = point_cloud.point_data.get_timestamp_min()?;
    info!("Timestamp max {:?}", timestamp_max);

    Ok(())
}
