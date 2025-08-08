use crate::error::Error;
use chrono::Utc;
use epoint::transform::translate;
use nalgebra::Vector3;
use std::path::Path;
use std::time::Instant;
use tracing::info;

pub fn run(file_path: impl AsRef<Path>) -> Result<(), Error> {
    info!("Start statistics");

    let now = Instant::now();
    /*let (point_cloud, _las_read_info) = epoint::io::LasReader::from_path(file_path)?
    .finish()?;*/
    /*let point_cloud = epoint::io::EpointReader::from_path(file_path)?
    .finish()?;*/

    let acquisition_start_timestamps = vec![
        Utc::now(),
        //Utc::now() + Duration::hours(1),
        //Utc::now() + Duration::hours(2),
        //Utc::now() + Duration::hours(3),
        //Utc::now() + Duration::hours(4),
    ];

    let reader = epoint::io::E57Reader::from_path(file_path)?
        .with_acquisition_start_timestamps(acquisition_start_timestamps);
    let point_cloud = reader.finish()?;
    info!("Read point cloud in {}s", now.elapsed().as_secs());
    let translated_point_cloud = translate(&point_cloud, Vector3::new(0.0, 0.0, 0.79))?;

    // info!("Number of points: {}\n", point_cloud.size());

    Ok(())
}
