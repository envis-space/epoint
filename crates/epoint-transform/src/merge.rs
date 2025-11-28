use ecoord::TransformTree;
use epoint_core::{PointCloud, PointCloudInfo, PointDataColumnType};
use polars::datatypes::{DataType, PlHashSet};

use crate::Error::{ContainsNoPoints, DifferentPointCloudInfos};
use polars::prelude::{IntoLazy, LazyFrame, concat};

use crate::error::Error;

pub fn merge(point_clouds: Vec<PointCloud>) -> Result<PointCloud, Error> {
    if point_clouds.is_empty() {
        return Err(ContainsNoPoints);
    }
    let info_set: PlHashSet<&PointCloudInfo> = point_clouds.iter().map(|x| x.info()).collect();
    if info_set.len() > 1 {
        return Err(DifferentPointCloudInfos);
    }
    let point_cloud_info = point_clouds.first().expect("must contain").info().clone();

    let transform_tree: Vec<TransformTree> = point_clouds
        .iter()
        .map(|p| p.transform_tree().clone())
        .collect();
    let merged_transform_tree = ecoord::merge(&transform_tree)?;

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
                    .unwrap()
                    .take_materialized_series();
                df.replace(PointDataColumnType::FrameId.as_str(), casted)
                    .unwrap();
            }

            df.lazy()
        })
        .collect();
    let mut merged_data_frame = concat(data_frame, Default::default())?.collect()?;

    let frame_id_column = merged_data_frame.column(PointDataColumnType::FrameId.as_str());
    if let Ok(frame_id_column) = frame_id_column {
        let casted = frame_id_column
            .to_owned()
            .cast(&PointDataColumnType::FrameId.data_frame_data_type())
            .unwrap()
            .take_materialized_series();
        merged_data_frame
            .replace(PointDataColumnType::FrameId.as_str(), casted)
            .unwrap();
    }

    let merged_point_cloud =
        PointCloud::from_data_frame(merged_data_frame, point_cloud_info, merged_transform_tree)?;
    Ok(merged_point_cloud)
}
