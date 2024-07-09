use crate::Error;
use chrono::{TimeZone, Timelike, Utc};
use epoint_core::PointCloud;
use las::{GpsTimeType, Write};
use rayon::prelude::*;
use std::fmt::Debug;
use std::io::Seek;

pub fn write_las_format<W: 'static + std::io::Write + Seek + Debug + Send>(
    writer: W,
    point_cloud: &PointCloud,
) -> Result<(), Error> {
    let center = point_cloud.point_data.get_local_center();

    let mut builder = las::Builder::from((1, 4));
    builder.point_format = las::point::Format::new(0).unwrap();
    builder.point_format.has_gps_time = point_cloud.contains_timestamps();
    builder.point_format.has_color = point_cloud.contains_colors();
    //builder.point_format.is_extended = false;

    builder.transforms.x.offset = center.x;
    // builder.transforms.x.scale = 1.0;
    builder.transforms.y.offset = center.y;
    builder.transforms.z.offset = center.z;
    builder.gps_time_type = GpsTimeType::Standard;

    let header = builder.into_header().unwrap();

    //header.transforms = las::Transform::default();

    // header.point_format().is_compressed = true;
    let mut las_writer = las::Writer::new(writer, header)?;

    let converted_timestamps = if point_cloud.contains_timestamps() {
        // this calculation should be adjusted gps time (see: https://groups.google.com/g/lastools/c/_9TxnjoghGM)
        let base_time = Utc.with_ymd_and_hms(1980, 1, 6, 0, 0, 0).unwrap(); // `2014-07-08T09:10:11Z`
        let values: Vec<f64> = point_cloud
            .point_data
            .get_all_timestamps()?
            .par_iter()
            .map(|t| {
                ((*t - base_time).num_seconds() - 1_000_000_000) as f64
                    + (t.nanosecond() as f64 / 1.0e-9)
            })
            .collect();
        Some(values)
    } else {
        None
    };

    let converted_colors = if point_cloud.contains_colors() {
        let values: Vec<las::Color> = point_cloud
            .point_data
            .get_all_colors()?
            .par_iter()
            .map(|c| las::Color::new(c.red, c.green, c.blue))
            .collect();
        Some(values)
    } else {
        None
    };

    let converted_intensity_values = point_cloud.point_data.get_intensity_values().ok();

    let converted_points: Vec<las::Point> = point_cloud
        .point_data
        .get_all_points()
        .par_iter()
        .enumerate()
        .map(|(i, p)| las::Point {
            x: p.x,
            y: p.y,
            z: p.z,
            gps_time: converted_timestamps
                .as_ref()
                .and_then(|v| v.get(i).cloned()),
            intensity: converted_intensity_values.map_or(0, |v| v.get(i).unwrap() as u16),
            color: converted_colors.as_ref().and_then(|v| v.get(i).cloned()),
            ..Default::default()
        })
        .collect();

    for current_point in converted_points {
        las_writer.write(current_point)?;
    }

    las_writer.close()?;
    Ok(())
}
