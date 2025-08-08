use crate::Error;
use crate::Error::InvalidVersion;
use crate::las::LasVersion;
use crate::las::read::LasReadInfo;
use epoint_core::{PointCloud, PointDataColumnType};
use las::Version;

use polars::prelude::DataFrame;
use polars::prelude::*;
use rayon::prelude::*;
use std::fmt::Debug;
use std::io::{BufReader, Seek};

pub fn import_point_cloud_from_las_reader<R: std::io::Read + Seek + Send + 'static + Debug>(
    reader: R,
    normalize_colors: bool,
) -> Result<(PointCloud, LasReadInfo), Error> {
    let mut las_reader = las::Reader::new(BufReader::new(reader))?;
    let mut las_points = Vec::new();
    let point_count = las_reader.read_all_points_into(&mut las_points)?;

    let mut point_data_columns = vec![
        Column::new(
            PointDataColumnType::X.into(),
            las_points.par_iter().map(|p| p.x).collect::<Vec<f64>>(),
        ),
        Column::new(
            PointDataColumnType::Y.into(),
            las_points.par_iter().map(|p| p.y).collect::<Vec<f64>>(),
        ),
        Column::new(
            PointDataColumnType::Z.into(),
            las_points.par_iter().map(|p| p.z).collect::<Vec<f64>>(),
        ),
        Column::new(
            PointDataColumnType::Intensity.into(),
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

        let color_red_column = Column::new(
            PointDataColumnType::ColorRed.into(),
            las_points
                .par_iter()
                .map(|p| p.color.unwrap_or_default().red * normalization_factor)
                .collect::<Vec<u16>>(),
        );
        point_data_columns.push(color_red_column);

        let color_green_column = Column::new(
            PointDataColumnType::ColorGreen.into(),
            las_points
                .par_iter()
                .map(|p| p.color.unwrap_or_default().green * normalization_factor)
                .collect::<Vec<u16>>(),
        );
        point_data_columns.push(color_green_column);

        let color_blue_column = Column::new(
            PointDataColumnType::ColorBlue.into(),
            las_points
                .par_iter()
                .map(|p| p.color.unwrap_or_default().blue * normalization_factor)
                .collect::<Vec<u16>>(),
        );
        point_data_columns.push(color_blue_column);
    }

    let point_data = DataFrame::new(point_data_columns)?;
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
