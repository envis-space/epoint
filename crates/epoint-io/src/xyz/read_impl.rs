use crate::Error::InvalidFileExtension;
use crate::{Error, FILE_EXTENSION_XYZ_FORMAT};
use ecoord::TransformTree;
use epoint_core::{PointCloud, PointCloudInfo, PointDataColumnType};
use polars::prelude::*;
use std::path::Path;

pub fn read_point_cloud_from_xyz_file(
    file_path: impl AsRef<Path>,
    separator: u8,
) -> Result<PointCloud, Error> {
    let data_frame = read_data_frame_from_xyz_file(&file_path, separator)?;

    let point_cloud = PointCloud::from_data_frame(
        data_frame,
        PointCloudInfo::default(),
        TransformTree::default(),
    )?;
    Ok(point_cloud)
}

pub fn read_data_frame_from_xyz_file(
    file_path: impl AsRef<Path>,
    separator: u8,
) -> Result<DataFrame, Error> {
    if file_path.as_ref().extension().unwrap() != FILE_EXTENSION_XYZ_FORMAT {
        return Err(InvalidFileExtension(
            file_path
                .as_ref()
                .to_owned()
                .extension()
                .unwrap_or_default()
                .to_str()
                .unwrap_or_default()
                .to_string(),
        ));
    }

    // TODO: maybe add all fields
    let schema_modifier = |mut schema: Schema| {
        if schema.contains(PointDataColumnType::Id.as_str()) {
            schema.with_column(
                PointDataColumnType::Id.as_str().into(),
                PointDataColumnType::Id.data_frame_data_type(),
            );
        }
        if schema.contains(PointDataColumnType::FrameId.as_str()) {
            schema.with_column(
                PointDataColumnType::FrameId.as_str().into(),
                PointDataColumnType::FrameId.data_frame_data_type(),
            );
        }

        if schema.contains(PointDataColumnType::TimestampNanoSecond.as_str()) {
            schema.with_column(
                PointDataColumnType::TimestampNanoSecond.as_str().into(),
                PointDataColumnType::TimestampNanoSecond.data_frame_data_type(),
            );
        }

        if schema.contains(PointDataColumnType::Intensity.as_str()) {
            schema.with_column(
                PointDataColumnType::Intensity.as_str().into(),
                PointDataColumnType::Intensity.data_frame_data_type(),
            );
        }

        if schema.contains(PointDataColumnType::ColorRed.as_str()) {
            schema.with_column(
                PointDataColumnType::ColorRed.as_str().into(),
                PointDataColumnType::ColorRed.data_frame_data_type(),
            );
        }
        if schema.contains(PointDataColumnType::ColorGreen.as_str()) {
            schema.with_column(
                PointDataColumnType::ColorGreen.as_str().into(),
                PointDataColumnType::ColorGreen.data_frame_data_type(),
            );
        }
        if schema.contains(PointDataColumnType::ColorBlue.as_str()) {
            schema.with_column(
                PointDataColumnType::ColorBlue.as_str().into(),
                PointDataColumnType::ColorBlue.data_frame_data_type(),
            );
        }

        Ok(schema)
    };

    let file_polars_path: PlPath = PlPath::Local(Arc::from(file_path.as_ref()));
    let data_frame = LazyCsvReader::new(file_polars_path)
        .with_separator(separator)
        .with_schema_modify(schema_modifier)?
        .finish()?
        .select([all().as_expr()])
        .collect()?;

    /*let _frame_id_series = data_frame.column(PointDataColumnType::FrameId.as_str());
    if let Ok(frame_id_series) = frame_id_series {
        let casted = frame_id_series
            .to_owned()
            .cast(&DataType::Categorical(None))
            .unwrap();
        data_frame
            .replace(PointDataColumnNames::FrameId.as_str(), casted)
            .unwrap();
    }
    let time_nanoseconds_series =
        data_frame.column(PointDataColumnNames::TimestampNanoSeconds.as_str());
    if let Ok(time_nanoseconds_series) = time_nanoseconds_series {
        let casted = time_nanoseconds_series
            .to_owned()
            .cast(&DataType::UInt32)
            .unwrap();
        data_frame
            .replace(PointDataColumnNames::TimestampNanoSeconds.as_str(), casted)
            .unwrap();
    }
    let intensity_series = data_frame.column(PointDataColumnNames::Intensity.as_str());
    if let Ok(intensity_series) = intensity_series {
        let casted = intensity_series
            .to_owned()
            .cast(&DataType::Float32)
            .unwrap();
        data_frame
            .replace(PointDataColumnNames::Intensity.as_str(), casted)
            .unwrap();
    }*/

    Ok(data_frame)
}
