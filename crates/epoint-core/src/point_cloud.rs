use crate::error::Error;

use crate::{data_frame_utils, PointCloudInfo, PointDataColumnNames, PointDataColumns};
use chrono::{DateTime, TimeZone, Utc};
use ecoord::{FrameId, ReferenceFrames};
use nalgebra;
use nalgebra::Point3;
use polars::prelude::DataFrame;

use polars::prelude::*;
use rayon::prelude::*;

#[derive(Debug, Clone, PartialEq)]
pub struct PointCloud {
    pub(crate) point_data: DataFrame,
    pub(crate) info: PointCloudInfo,
    pub(crate) reference_frames: ReferenceFrames,
}

impl PointCloud {
    pub fn new(
        point_data: PointDataColumns,
        info: PointCloudInfo,
        reference_frames: ReferenceFrames,
    ) -> Result<Self, Error> {
        let point_data = point_data.get_as_data_frame();
        data_frame_utils::check_data_integrity(&point_data, &info, &reference_frames)?;

        Ok(Self {
            point_data,
            info,
            reference_frames,
        })
    }

    pub fn from_data_frame(
        point_data: DataFrame,
        info: PointCloudInfo,
        reference_frames: ReferenceFrames,
    ) -> Result<Self, Error> {
        /*assert!(
            frames.contains_frame(&info.frame_id),
            "Reference frames must contain frame id '{}' of point cloud data.",
            info.frame_id
        );*/

        data_frame_utils::check_data_integrity(&point_data, &info, &reference_frames)?;
        Ok(Self {
            point_data,
            info,
            reference_frames,
        })
    }

    pub fn point_data(&self) -> &DataFrame {
        &self.point_data
    }

    pub fn info(&self) -> &PointCloudInfo {
        &self.info
    }

    pub fn reference_frames(&self) -> &ReferenceFrames {
        &self.reference_frames
    }

    pub fn get_frame_ids(&self) -> Option<Vec<FrameId>> {
        data_frame_utils::extract_frame_ids(&self.point_data)
    }

    pub fn size(&self) -> usize {
        self.point_data.height()
    }

    pub fn frame_id(&self) -> Option<&FrameId> {
        self.info.frame_id.as_ref()
    }

    pub fn get_point_data(&self) -> &DataFrame {
        &self.point_data
    }

    /// Returns the minimum point of the [AABB](https://en.wikipedia.org/wiki/Minimum_bounding_box#Axis-aligned_minimum_bounding_box).
    pub fn get_local_min(&self) -> Point3<f64> {
        let selected_df_row = self.point_data.min();
        let x: f64 = selected_df_row
            .column(PointDataColumnNames::X.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        let y: f64 = selected_df_row
            .column(PointDataColumnNames::Y.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        let z: f64 = selected_df_row
            .column(PointDataColumnNames::Z.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();

        Point3::new(x, y, z)
    }

    /// Returns the maximum point of the [AABB](https://en.wikipedia.org/wiki/Minimum_bounding_box#Axis-aligned_minimum_bounding_box).
    ///
    ///
    pub fn get_local_max(&self) -> Point3<f64> {
        let selected_df_row = self.point_data.max();
        let x: f64 = selected_df_row
            .column(PointDataColumnNames::X.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        let y: f64 = selected_df_row
            .column(PointDataColumnNames::Y.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        let z: f64 = selected_df_row
            .column(PointDataColumnNames::Z.as_str())
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();

        Point3::new(x, y, z)
    }

    /// Returns all points as a vector in the local coordinate frame.
    ///
    ///
    pub fn get_all_points(&self) -> Vec<Point3<f64>> {
        data_frame_utils::extract_points(&self.point_data)
    }

    pub fn contains_timestamps(&self) -> bool {
        self.point_data
            .column(PointDataColumnNames::TimestampSeconds.as_str())
            .is_ok()
            && self
                .point_data
                .column(PointDataColumnNames::TimestampNanoSeconds.as_str())
                .is_ok()
    }

    pub fn get_all_timesamps(&self) -> Option<Vec<DateTime<Utc>>> {
        if !self.contains_timestamps() {
            return None;
        }

        let timestamp_sec_series = self
            .point_data
            .column(PointDataColumnNames::TimestampSeconds.as_str())
            .ok()?
            .i64()
            .unwrap();

        let timestamp_nanosec_series = self
            .point_data
            .column(PointDataColumnNames::TimestampNanoSeconds.as_str())
            .ok()?
            .u32()
            .unwrap();

        let times: Vec<DateTime<Utc>> = timestamp_sec_series
            .into_iter()
            .zip(timestamp_nanosec_series.into_iter())
            .map(|t| Utc.timestamp_opt(t.0.unwrap(), t.1.unwrap()).unwrap())
            .collect();
        Some(times)
    }

    pub fn get_median_time(&self) -> Option<DateTime<Utc>> {
        let mut all_time = self.get_all_timesamps()?;
        all_time.sort();
        let mid = all_time.len() / 2;
        Some(all_time[mid])
    }

    pub fn set_reference_frames(&mut self, reference_frames: ReferenceFrames) {
        self.reference_frames = reference_frames;
    }

    pub fn update_points(
        &mut self,
        points: Vec<Point3<f64>>,
        frame_id: FrameId,
    ) -> Result<(), Error> {
        self.point_data = data_frame_utils::update_points(self.point_data(), points)?;
        self.info.frame_id = Some(frame_id);

        Ok(())
    }
}
