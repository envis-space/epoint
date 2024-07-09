use crate::Error;
use epoint_core::{PointCloud, PointDataColumnType};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use epoint_core::Error::NoData;
use polars::prelude::{all, col, IntoLazy, NamedFrom, PolarsResult, Series};

fn map_string_to_color_value(s: Series, offset: usize) -> PolarsResult<Option<Series>> {
    // TODO: add error handling for non-string
    // TODO: add error handling for allowed offset argument range
    let number_of_values: Vec<Option<&str>> = s.str()?.into_iter().collect();
    let value: &str = s.str()?.into_iter().next().flatten().unwrap();

    let mut hasher = DefaultHasher::new();
    hasher.write(value.as_bytes());
    let hasher_finish = hasher.finish();
    // println!("{:?}", hasher_finish);

    let vector = hasher_finish.to_le_bytes();
    let number = ((vector[offset] as u16) << 8) | vector[offset + 1] as u16;

    let new_series: Series = Series::new("", vec![number; number_of_values.len()]);
    Ok(Some(new_series))
}

pub fn colorize_by_column_hash(
    point_cloud: &PointCloud,
    column_name: &str,
) -> Result<PointCloud, Error> {
    // TODO: add error handling

    let df = point_cloud
        .point_data
        .data_frame
        .clone()
        .lazy()
        .group_by([column_name])
        .agg([
            all(),
            col(column_name)
                .apply(|s| map_string_to_color_value(s, 0), Default::default())
                .alias(PointDataColumnType::ColorRed.as_str()),
            col(column_name)
                .apply(|s| map_string_to_color_value(s, 2), Default::default())
                .alias(PointDataColumnType::ColorGreen.as_str()),
            col(column_name)
                .apply(|s| map_string_to_color_value(s, 4), Default::default())
                .alias(PointDataColumnType::ColorBlue.as_str()),
        ])
        .explode([all().exclude([column_name])])
        .select([all().exclude([column_name]), col(column_name)])
        .collect()?;
    // println!("{}", df);

    let colorized_point_cloud = PointCloud::from_data_frame(
        df,
        point_cloud.info().clone(),
        point_cloud.reference_frames().clone(),
    )?;
    Ok(colorized_point_cloud)
}

pub fn colorize_by_intensity_in_place(point_cloud: &mut PointCloud) -> Result<(), Error> {
    let intensity_min = point_cloud
        .point_data
        .get_intensity_min()?
        .ok_or(NoData(""))?;
    let intensity_max = point_cloud
        .point_data
        .get_intensity_max()?
        .ok_or(NoData(""))?;
    let intensity_range = intensity_max - intensity_min;
    // println!("Intensity range: {:?}-{:?}", intensity_min, intensity_max);

    let intensity_values = point_cloud.point_data.get_intensity_values()?;
    let colors: Vec<palette::Srgb<u16>> = intensity_values
        .into_no_null_iter()
        .map(|i| {
            let scaled = (u16::MAX as f32) * ((i - intensity_min) / intensity_range);

            palette::Srgb::new(scaled as u16, scaled as u16, scaled as u16)
        })
        .collect();

    point_cloud.point_data.add_colors(colors)?;
    Ok(())
}
