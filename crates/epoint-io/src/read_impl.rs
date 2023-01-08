use crate::error::Error;
use epoint_core::PointDataColumnNames;
use polars::datatypes::DataType;
use polars::prelude::{all, DataFrame, LazyCsvReader};
use std::path::Path;

pub fn read_data_frame_from_xyz_file(file_path: impl AsRef<Path>) -> Result<DataFrame, Error> {
    assert_eq!(file_path.as_ref().extension().unwrap(), "xyz");

    let mut data_frame = LazyCsvReader::new(file_path)
        .with_delimiter(b' ')
        .finish()?
        .select([all()])
        .collect()?;

    let frame_id_series = data_frame.column(PointDataColumnNames::FrameId.as_str());
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
    }

    Ok(data_frame)
}
