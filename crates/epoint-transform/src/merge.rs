use ecoord::ReferenceFrames;
use epoint_core::{PointCloud, PointDataColumnNames};
use polars::datatypes::DataType;

use polars::prelude::{concat, IntoLazy, LazyFrame};

use crate::error::Error;

pub fn merge(point_clouds: Vec<PointCloud>) -> Result<PointCloud, Error> {
    assert!(!point_clouds.is_empty(), "Must contain point clouds");

    // TODO: check if equal
    let point_cloud_info = point_clouds.first().unwrap().info().clone();

    let reference_frames: Vec<ReferenceFrames> = point_clouds
        .iter()
        .map(|p| p.reference_frames().clone())
        .collect();
    let merged_reference_frames = ecoord::merge(&reference_frames);

    let data_frame: Vec<LazyFrame> = point_clouds
        .iter()
        .map(|p| {
            // back casting to Utf8, as something with merging the string cache doesn't work
            let mut df = p.point_data().clone();
            let casted = df
                .column(PointDataColumnNames::FrameId.as_str())
                .unwrap()
                .cast(&DataType::Utf8)
                .unwrap();
            df.replace(PointDataColumnNames::FrameId.as_str(), casted)
                .unwrap();
            df.lazy()
        })
        .collect();
    let mut merged_data_frame = concat(data_frame, true, true).unwrap().collect().unwrap();

    let frame_id_series = merged_data_frame.column(PointDataColumnNames::FrameId.as_str());
    if let Ok(frame_id_series) = frame_id_series {
        let casted = frame_id_series
            .to_owned()
            .cast(&DataType::Categorical(None))
            .unwrap();
        merged_data_frame
            .replace(PointDataColumnNames::FrameId.as_str(), casted)
            .unwrap();
    }

    let merged_point_cloud =
        PointCloud::from_data_frame(merged_data_frame, point_cloud_info, merged_reference_frames)?;
    Ok(merged_point_cloud)
}
