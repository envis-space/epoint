use crate::error::Error;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};

use crate::{PointCloudInfo, PointDataColumnType, PointDataColumns};

use ecoord::{FrameId, ReferenceFrames};
use nalgebra;
use nalgebra::Point3;

use polars::prelude::DataFrame;

use crate::point_data::PointData;
use crate::Error::{
    MultipleFrameIdDefinitions, NoFrameIdDefinition, NoFrameIdDefinitions, NoIdColumn,
};
use polars::prelude::*;

#[derive(Debug, Clone, PartialEq)]
pub struct PointCloud {
    pub point_data: PointData,
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
        if point_data
            .column(PointDataColumnType::FrameId.as_str())
            .is_ok()
            && info.frame_id.is_some()
        {
            return Err(MultipleFrameIdDefinitions);
        }

        let point_data_new = PointData::new(point_data)?; // TODO: simplify

        Ok(Self {
            point_data: point_data_new,
            info,
            reference_frames,
        })
    }

    pub fn from_data_frame(
        point_data: DataFrame,
        info: PointCloudInfo,
        reference_frames: ReferenceFrames,
    ) -> Result<Self, Error> {
        if point_data
            .column(PointDataColumnType::FrameId.as_str())
            .is_ok()
            && info.frame_id.is_some()
        {
            return Err(MultipleFrameIdDefinitions);
        }
        let point_data_new = PointData::new(point_data)?; // TODO: simplify

        Ok(Self {
            point_data: point_data_new,
            info,
            reference_frames,
        })
    }
}

impl PointCloud {
    pub fn point_data(&self) -> &PointData {
        &self.point_data
    }

    pub fn info(&self) -> &PointCloudInfo {
        &self.info
    }

    pub fn reference_frames(&self) -> &ReferenceFrames {
        &self.reference_frames
    }

    pub fn size(&self) -> usize {
        self.point_data.height()
    }

    pub fn info_frame_id(&self) -> Option<&FrameId> {
        self.info.frame_id.as_ref()
    }

    pub fn get_distinct_frame_ids(&self) -> Option<HashSet<FrameId>> {
        if let Some(frame_id) = &self.info.frame_id {
            let frame_ids = HashSet::from([frame_id.clone()]);
            return Some(frame_ids);
        }

        if self.point_data.contains_frame_id_column() {
            let frame_ids = self
                .point_data
                .get_distinct_frame_ids()
                .expect("must be derivable");
            return Some(frame_ids);
        }

        None
    }
}

impl PointCloud {
    pub fn contains_ids(&self) -> bool {
        self.point_data.contains_id_column()
    }

    pub fn contains_frame_ids(&self) -> bool {
        self.info.frame_id.is_some() || !self.point_data.contains_frame_id_column()
    }
    pub fn contains_timestamps(&self) -> bool {
        self.point_data.contains_timestamps()
    }

    pub fn contains_beam_origin(&self) -> bool {
        self.point_data.contains_beam_origin()
    }

    pub fn contains_colors(&self) -> bool {
        self.point_data.contains_colors()
    }

    pub fn set_reference_frames(&mut self, reference_frames: ReferenceFrames) {
        self.reference_frames = reference_frames;
    }

    pub fn set_info_frame_id(&mut self, frame_id: Option<FrameId>) {
        self.info.frame_id = frame_id;
    }

    pub fn update_points(
        &mut self,
        points: Vec<Point3<f64>>,
        frame_id: Option<FrameId>,
    ) -> Result<(), Error> {
        self.point_data.update_points_in_place(points)?;
        self.info.frame_id = frame_id;

        Ok(())
    }

    pub fn derive_spherical_points(&mut self) -> Result<(), Error> {
        self.point_data.derive_spherical_points()?;

        Ok(())
    }

    pub fn filter_by_id_range(
        &self,
        id_min: Option<u64>,
        id_max: Option<u64>,
    ) -> Result<PointCloud, Error> {
        if !self.contains_ids() {
            return Err(NoIdColumn);
        }

        let mut filter_predicate = col(PointDataColumnType::Id.as_str());
        if let Some(id_min) = id_min {
            filter_predicate = filter_predicate.gt_eq(lit(id_min));
        }
        if let Some(id_max) = id_max {
            filter_predicate =
                filter_predicate.and(col(PointDataColumnType::Id.as_str()).lt_eq(id_max));
        }

        let point_data = self
            .point_data
            .data_frame
            .clone()
            .lazy()
            .filter(filter_predicate)
            .collect()?;

        let filtered_point_cloud = PointCloud::from_data_frame(
            point_data,
            self.info.clone(),
            self.reference_frames.clone(),
        )?;
        Ok(filtered_point_cloud)
    }

    pub fn filter_by_frame_id(&self, frame_id: &FrameId) -> Result<PointCloud, Error> {
        if !self
            .get_distinct_frame_ids()
            .ok_or(NoFrameIdDefinitions)?
            .contains(frame_id)
        {
            return Err(NoFrameIdDefinition(frame_id.clone()));
        }

        let filter_predicate = col(PointDataColumnType::FrameId.as_str())
            .cast(DataType::String)
            .eq(lit(frame_id.clone().to_string().as_str()));

        let point_data = self
            .point_data
            .data_frame
            .clone()
            .lazy()
            .filter(filter_predicate)
            .collect()?;

        let filtered_point_cloud = PointCloud::from_data_frame(
            point_data,
            self.info.clone(),
            self.reference_frames.clone(),
        )?;
        Ok(filtered_point_cloud)
    }

    pub fn filter_by_row_indices(&self, row_indices: HashSet<usize>) -> Result<PointCloud, Error> {
        let filtered_point_data = self.point_data.filter_by_row_indices(row_indices)?;

        let filtered_point_cloud = PointCloud::from_data_frame(
            filtered_point_data.data_frame,
            self.info.clone(),
            self.reference_frames.clone(),
        )?;
        Ok(filtered_point_cloud)
    }

    pub fn filter_by_boolean_mask(&self, mask: &Vec<bool>) -> Result<PointCloud, Error> {
        let mask_series: Series = mask.iter().collect();
        let filtered_point_data = self
            .point_data
            .filter_by_boolean_mask(mask_series.bool()?)?;

        let filtered_point_cloud = PointCloud::from_data_frame(
            filtered_point_data.data_frame,
            self.info.clone(),
            self.reference_frames.clone(),
        )?;
        Ok(filtered_point_cloud)
    }

    pub fn filter_by_bounds(
        &self,
        bound_min: Point3<f64>,
        bound_max: Point3<f64>,
    ) -> Result<Option<PointCloud>, Error> {
        let filtered_point_data = self.point_data.filter_by_bounds(bound_min, bound_max)?;

        let result = if let Some(filtered_point_data) = filtered_point_data {
            let filtered_point_cloud = PointCloud::from_data_frame(
                filtered_point_data.data_frame,
                self.info.clone(),
                self.reference_frames.clone(),
            )?;
            Some(filtered_point_cloud)
        } else {
            None
        };

        Ok(result)
    }

    pub fn filter_by_beam_length(
        &self,
        beam_length_min: f64,
        beam_length_max: f64,
    ) -> Result<Option<PointCloud>, Error> {
        let filtered_point_data = self
            .point_data
            .filter_by_beam_length(beam_length_min, beam_length_max)?;

        let result = if let Some(filtered_point_data) = filtered_point_data {
            let filtered_point_cloud = PointCloud::from_data_frame(
                filtered_point_data.data_frame,
                self.info.clone(),
                self.reference_frames.clone(),
            )?;
            Some(filtered_point_cloud)
        } else {
            None
        };

        Ok(result)
    }
}

impl PointCloud {
    /// Resolves the frame-dependent and time-dependent points of the point cloud to a target frame.
    ///
    /// The points are partitioned by frame ids and timestamps and a coordinate transform is
    /// derived for each partition. It must be
    ///
    pub fn resolve_to_frame(&mut self, target_frame_id: FrameId) -> Result<(), Error> {
        if self.info.frame_id.is_none() && !self.point_data.contains_frame_id_column() {
            return Err(NoFrameIdDefinitions);
        }

        // interesting: https://stackoverflow.com/a/65287197

        let mut partition_columns: Vec<&str> = Vec::new();
        if self.point_data.contains_frame_id_column() {
            partition_columns.push(PointDataColumnType::FrameId.as_str());
        }
        if self.point_data.contains_timestamp_sec_column() {
            partition_columns.push(PointDataColumnType::TimestampSeconds.as_str());
        }
        if self.point_data.contains_timestamp_nanosec_column() {
            partition_columns.push(PointDataColumnType::TimestampNanoSeconds.as_str());
        }

        let partitioned: Vec<DataFrame> = if partition_columns.is_empty() {
            vec![self.point_data.data_frame.clone()]
        } else {
            self.point_data
                .data_frame
                .clone()
                .partition_by(partition_columns, true)?
        };

        //
        let partitioned: HashMap<(FrameId, Option<DateTime<Utc>>), DataFrame> = partitioned
            .into_iter()
            .map(|df| {
                let point_data = PointData::new_unchecked(df);
                // get either the frame id per point or the general frame id in the point cloud info
                let frame_ids_series = point_data.extract_frame_ids();
                let frame_id = frame_ids_series.map_or_else(
                    |_| self.info.frame_id.clone().unwrap(),
                    |f| f.first().unwrap().clone(),
                );

                let timestamp = point_data
                    .extract_timestamps()
                    .ok()
                    .map(|t| *t.first().unwrap());
                ((frame_id, timestamp), point_data.data_frame)
            })
            .collect();

        /*partitioned.sort_by_key(|k| {
            k.column(PointDataColumnNames::TimestampSeconds.as_str())
                .unwrap()
                .i64()
                .unwrap()
                .get(0)
                .unwrap()
                * 1000000000
                + k.column(PointDataColumnNames::TimestampNanoSeconds.as_str())
                    .unwrap()
                    .u32()
                    .unwrap()
                    .get(0)
                    .unwrap() as i64
        });*/

        /*     let frame_id = data_frame_utils::extract_frame_ids(point_data)
        .unwrap()
        .first()
        .unwrap()
        .clone();*/

        let partitioned: Vec<DataFrame> = partitioned
            .iter()
            .map(|((current_frame_id, current_timestamp), df)| {
                let mut point_data = PointData::new_unchecked(df.clone());
                point_data
                    .resolve_data_frame(
                        &self.reference_frames,
                        current_timestamp,
                        current_frame_id,
                        &target_frame_id,
                    )
                    .unwrap();

                point_data.data_frame
            })
            .collect();
        // dbg!(&partitioned);

        let partitioned_lazy: Vec<LazyFrame> =
            partitioned.iter().map(|d| d.clone().lazy()).collect();

        let mut merged_again = concat(partitioned_lazy, Default::default())
            .unwrap()
            .collect()
            .unwrap();

        // sort by timestamp, if available without id
        if merged_again
            .column(PointDataColumnType::TimestampSeconds.as_str())
            .is_ok()
            && merged_again
                .column(PointDataColumnType::TimestampNanoSeconds.as_str())
                .is_ok()
            && merged_again
                .column(PointDataColumnType::FrameId.as_str())
                .is_err()
        {
            merged_again = merged_again
                .sort(
                    [
                        PointDataColumnType::TimestampSeconds.as_str(),
                        PointDataColumnType::TimestampNanoSeconds.as_str(),
                    ],
                    SortMultipleOptions::default().with_maintain_order(true),
                )
                .expect("");
        }
        // sort by id
        if merged_again
            .column(PointDataColumnType::Id.as_str())
            .is_ok()
        {
            merged_again = merged_again
                .sort(
                    [PointDataColumnType::Id.as_str()],
                    SortMultipleOptions::default().with_maintain_order(true),
                )
                .expect("");
        }

        self.point_data.data_frame = merged_again;
        self.info.frame_id = Some(target_frame_id);

        Ok(())
    }
}
