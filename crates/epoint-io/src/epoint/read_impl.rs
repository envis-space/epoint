use crate::error::Error;

use epoint_core::PointDataColumnType;
use polars::prelude::DataFrame;
use polars::prelude::*;

use strum::IntoEnumIterator;

pub fn cast_data_frame(data_frame: DataFrame) -> Result<DataFrame, Error> {
    let mut column_casting_expr: Vec<Expr> = Vec::new();
    for column in PointDataColumnType::iter() {
        add_column_cast_in_place(&data_frame, column, &mut column_casting_expr);
    }

    let casted_data_frame = data_frame
        .clone()
        .lazy()
        .select(column_casting_expr)
        .collect()?;

    Ok(casted_data_frame)
}

fn add_column_cast_in_place(
    data_frame: &DataFrame,
    column: PointDataColumnType,
    column_casting_expr: &mut Vec<Expr>,
) {
    if data_frame.column(column.as_str()).is_ok() {
        column_casting_expr.push(col(column.as_str()).cast(column.data_frame_data_type()));
    }
}
