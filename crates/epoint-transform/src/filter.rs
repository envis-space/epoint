use crate::Error;
use epoint_core::PointCloud;
use polars::prelude::{IntoLazy, all, col};

pub fn filter_none_values_of_column_in_place(
    point_cloud: &mut PointCloud,
    column_name: &str,
) -> Result<(), Error> {
    let filtered_point_data = point_cloud
        .point_data
        .data_frame
        .clone()
        .lazy()
        .filter(col(column_name).is_not_null())
        .select([all().as_expr()])
        .collect()?;

    point_cloud.point_data.data_frame = filtered_point_data;

    Ok(())
}
