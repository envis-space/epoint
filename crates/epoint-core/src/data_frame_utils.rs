use ecoord::{FrameId, ReferenceFrames};
use nalgebra::Point3;

use crate::Error::{
    ColumnNameMisMatch, MultipleFrameIdDefinitions, NoData, ShapeMisMatch, TypeMisMatch,
};
use crate::{Error, PointCloudInfo, PointDataColumnNames};
use polars::datatypes::DataType;
use polars::frame::DataFrame;

use polars::prelude::*;
use rayon::prelude::*;

pub fn check_data_integrity(
    point_data: &DataFrame,
    info: &PointCloudInfo,
    _reference_frames: &ReferenceFrames,
) -> Result<(), Error> {
    if point_data
        .column(PointDataColumnNames::FrameId.as_str())
        .is_ok()
        && info.frame_id.is_some()
    {
        return Err(MultipleFrameIdDefinitions);
    }
    // if !reference_frames.get_frame_ids().is_empty() {
    //     assert!(
    //         info.frame_id.is_some(),
    //         "Frame for point cloud must be provided."
    //     );
    //
    //     if let Some(id) = &info.frame_id {
    //         assert!(reference_frames.contains_frame(&id.clone()), ".");
    //     }
    // }

    check_point_data_frame_validity(point_data)?;

    Ok(())
}

pub fn check_point_data_frame_validity(point_data: &DataFrame) -> Result<(), Error> {
    if point_data.is_empty() {
        return Err(NoData("point_data"));
    }

    let column_names = point_data.get_column_names();
    if column_names[0] != PointDataColumnNames::X.as_str() {
        return Err(ColumnNameMisMatch);
    }
    if column_names[1] != PointDataColumnNames::Y.as_str() {
        return Err(ColumnNameMisMatch);
    }
    if column_names[2] != PointDataColumnNames::Z.as_str() {
        return Err(ColumnNameMisMatch);
    }

    let data_types = point_data.dtypes();
    if data_types[0] != DataType::Float64 {
        return Err(TypeMisMatch("x"));
    }
    if data_types[1] != DataType::Float64 {
        return Err(TypeMisMatch("y"));
    }
    if data_types[2] != DataType::Float64 {
        return Err(TypeMisMatch("z"));
    }

    if let Ok(s) = point_data.column(PointDataColumnNames::FrameId.as_str()) {
        if s.dtype() != &DataType::Categorical(None) {
            return Err(TypeMisMatch("frame_id"));
        }
    }

    if let Ok(s) = point_data.column(PointDataColumnNames::TimestampSeconds.as_str()) {
        if s.dtype() != &DataType::Int64 {
            return Err(TypeMisMatch("timestamp_seconds"));
        }
    }

    if let Ok(s) = point_data.column(PointDataColumnNames::TimestampNanoSeconds.as_str()) {
        if s.dtype() != &DataType::UInt32 {
            return Err(TypeMisMatch("timestamp_nanoseconds"));
        }
    }

    if let Ok(s) = point_data.column(PointDataColumnNames::Intensity.as_str()) {
        if s.dtype() != &DataType::Float32 {
            return Err(TypeMisMatch("intensity"));
        }
    }

    Ok(())
}

pub fn extract_points(point_data: &DataFrame) -> Vec<Point3<f64>> {
    let x_series = point_data
        .column(PointDataColumnNames::X.as_str())
        .unwrap()
        .f64()
        .unwrap();
    let y_series = point_data
        .column(PointDataColumnNames::Y.as_str())
        .unwrap()
        .f64()
        .unwrap();
    let z_series = point_data
        .column(PointDataColumnNames::Z.as_str())
        .unwrap()
        .f64()
        .unwrap();

    let all_points: Vec<Point3<f64>> = (0..point_data.height() as usize)
        .into_par_iter()
        .map(|i: usize| {
            Point3::new(
                x_series.get(i).unwrap(),
                y_series.get(i).unwrap(),
                z_series.get(i).unwrap(),
            )
        })
        .collect();

    all_points
}

pub fn extract_frame_ids(point_data: &DataFrame) -> Option<Vec<FrameId>> {
    let frame_ids = point_data
        .column(PointDataColumnNames::FrameId.as_str())
        .ok()?
        .cast(&DataType::Utf8)
        .unwrap()
        .utf8()
        .unwrap()
        .into_no_null_iter()
        .map(|f| f.to_string().into())
        .collect();

    Some(frame_ids)
}

pub fn update_points(point_data: &DataFrame, points: Vec<Point3<f64>>) -> Result<DataFrame, Error> {
    if points.len() != point_data.height() {
        return Err(ShapeMisMatch);
    }

    let mut updated_point_data = point_data.clone();

    if updated_point_data
        .column(PointDataColumnNames::FrameId.as_str())
        .is_ok()
    {
        let _ = updated_point_data
            .drop_in_place(PointDataColumnNames::FrameId.as_str())
            .expect("Column should be successfully replaced");
    }

    let x_series = Series::new(
        PointDataColumnNames::X.as_str(),
        points.iter().map(|p| p.x).collect::<Vec<f64>>(),
    );
    let y_series = Series::new(
        PointDataColumnNames::Y.as_str(),
        points.iter().map(|p| p.y).collect::<Vec<f64>>(),
    );
    let z_series = Series::new(
        PointDataColumnNames::Z.as_str(),
        points.iter().map(|p| p.z).collect::<Vec<f64>>(),
    );
    updated_point_data.replace(PointDataColumnNames::X.as_str(), x_series)?;
    updated_point_data.replace(PointDataColumnNames::Y.as_str(), y_series)?;
    updated_point_data.replace(PointDataColumnNames::Z.as_str(), z_series)?;

    Ok(updated_point_data)
}
