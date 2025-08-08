use crate::error::Error;
use std::str::FromStr;

use epoint_core::PointDataColumnType;
use polars::prelude::DataFrame;
use polars::prelude::*;

pub fn cast_data_frame(data_frame: DataFrame) -> Result<DataFrame, Error> {
    let mut column_casting_expr: Vec<Expr> = Vec::new();
    for current_column_name in data_frame.get_column_names() {
        let column_type = PointDataColumnType::from_str(current_column_name.as_str()).ok();

        if let Some(column_type) = column_type {
            column_casting_expr
                .push(col(column_type.as_str()).cast(column_type.data_frame_data_type()));
        } else {
            column_casting_expr.push(col(current_column_name.as_str()));
        }
    }

    let casted_data_frame = data_frame
        .clone()
        .lazy()
        .select(column_casting_expr)
        .collect()?;

    Ok(casted_data_frame)
}
