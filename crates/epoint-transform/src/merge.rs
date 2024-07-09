use ecoord::ReferenceFrames;
use epoint_core::{PointCloud, PointCloudInfo, PointDataColumnType};
use polars::datatypes::DataType;
use polars::export::ahash::HashSet;

use crate::Error::{ContainsNoPoints, DifferentPointCloudInfos};
use polars::prelude::{concat, IntoLazy, LazyFrame};

use crate::error::Error;

pub fn merge(point_clouds: Vec<PointCloud>) -> Result<PointCloud, Error> {
    if point_clouds.is_empty() {
        return Err(ContainsNoPoints);
    }
    let info_set: HashSet<&PointCloudInfo> = point_clouds.iter().map(|x| x.info()).collect();
    if info_set.len() > 1 {
        return Err(DifferentPointCloudInfos);
    }
    let point_cloud_info = point_clouds.first().expect("must contain").info().clone();

    let reference_frames: Vec<ReferenceFrames> = point_clouds
        .iter()
        .map(|p| p.reference_frames().clone())
        .collect();
    let merged_reference_frames = ecoord::merge(&reference_frames)?;

    let data_frame: Vec<LazyFrame> = point_clouds
        .iter()
        .map(|p| {
            let mut df = p.point_data.data_frame.clone();
            // back casting to Utf8, as something with merging the string cache doesn't work
            if p.point_data.contains_frame_id_column() {
                let casted = df
                    .column(PointDataColumnType::FrameId.as_str())
                    .unwrap()
                    .cast(&DataType::String)
                    .unwrap();
                df.replace(PointDataColumnType::FrameId.as_str(), casted)
                    .unwrap();
            }

            df.lazy()
        })
        .collect();
    let mut merged_data_frame = concat(data_frame, Default::default())
        .unwrap()
        .collect()
        .unwrap();

    let frame_id_series = merged_data_frame.column(PointDataColumnType::FrameId.as_str());
    if let Ok(frame_id_series) = frame_id_series {
        let casted = frame_id_series
            .to_owned()
            .cast(&DataType::Categorical(None, Default::default()))
            .unwrap();
        merged_data_frame
            .replace(PointDataColumnType::FrameId.as_str(), casted)
            .unwrap();
    }

    let merged_point_cloud =
        PointCloud::from_data_frame(merged_data_frame, point_cloud_info, merged_reference_frames)?;
    Ok(merged_point_cloud)
}
