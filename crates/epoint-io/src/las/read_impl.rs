use crate::las::read::LasReadInfo;
use crate::las::LasVersion;
use crate::Error;
use crate::Error::InvalidVersion;
use epoint_core::{PointCloud, PointDataColumnType};
use las::{Read, Version};

use polars::export::rayon;
use polars::prelude::DataFrame;
use polars::prelude::*;
use polars::series::Series;
use rayon::prelude::*;
use std::fmt::Debug;
use std::io::{BufReader, Seek};

pub fn import_point_cloud_from_las_file<R: std::io::Read + Seek + Send + Debug>(
    reader: R,
    normalize_colors: bool,
) -> Result<(PointCloud, LasReadInfo), Error> {
    let mut las_reader = las::Reader::new(BufReader::new(reader))?;
    let las_points = las_reader.points().collect::<Result<Vec<_>, _>>()?;
    // println!("header: {:?}", las_reader.header());

    let mut point_data_columns = vec![
        Series::new(
            PointDataColumnType::X.as_str(),
            las_points.par_iter().map(|p| p.x).collect::<Vec<f64>>(),
        ),
        Series::new(
            PointDataColumnType::Y.as_str(),
            las_points.par_iter().map(|p| p.y).collect::<Vec<f64>>(),
        ),
        Series::new(
            PointDataColumnType::Z.as_str(),
            las_points.par_iter().map(|p| p.z).collect::<Vec<f64>>(),
        ),
        Series::new(
            PointDataColumnType::Intensity.as_str(),
            las_points
                .par_iter()
                .map(|p| p.intensity as f32)
                .collect::<Vec<f32>>(),
        ),
    ];
    if las_points.par_iter().all(|p| p.color.is_some()) {
        // check if normalization needed
        let normalization_factor = if normalize_colors
            && las_points.par_iter().map(|p| p.color.unwrap()).all(|c| {
                c.red <= u8::MAX as u16 && c.green <= u8::MAX as u16 && c.blue <= u8::MAX as u16
            }) {
            256
        } else {
            1
        };

        let color_red_series = Series::new(
            PointDataColumnType::ColorRed.as_str(),
            las_points
                .par_iter()
                .map(|p| p.color.unwrap_or_default().red * normalization_factor)
                .collect::<Vec<u16>>(),
        );
        point_data_columns.push(color_red_series);

        let color_green_series = Series::new(
            PointDataColumnType::ColorGreen.as_str(),
            las_points
                .par_iter()
                .map(|p| p.color.unwrap_or_default().green * normalization_factor)
                .collect::<Vec<u16>>(),
        );
        point_data_columns.push(color_green_series);

        let color_blue_series = Series::new(
            PointDataColumnType::ColorBlue.as_str(),
            las_points
                .par_iter()
                .map(|p| p.color.unwrap_or_default().blue * normalization_factor)
                .collect::<Vec<u16>>(),
        );
        point_data_columns.push(color_blue_series);
    }

    let point_data = DataFrame::new(point_data_columns).unwrap();

    let point_cloud =
        PointCloud::from_data_frame(point_data, Default::default(), Default::default())?;

    let version = get_version(&las_reader)?;
    let las_read_info = LasReadInfo { version };

    Ok((point_cloud, las_read_info))
}

fn get_version(las_reader: &las::Reader) -> Result<LasVersion, Error> {
    //let mut las_reader = las::Reader::new(self.reader.clone())?;
    let version = las_reader.header().version();

    match version {
        Version { major: 1, minor: 0 } => Ok(LasVersion::V1_0),
        Version { major: 1, minor: 1 } => Ok(LasVersion::V1_1),
        Version { major: 1, minor: 2 } => Ok(LasVersion::V1_2),
        Version { major: 1, minor: 3 } => Ok(LasVersion::V1_3),
        Version { major: 1, minor: 4 } => Ok(LasVersion::V1_4),
        _ => Err(InvalidVersion {
            major: version.major,
            minor: version.minor,
        }),
    }
}
