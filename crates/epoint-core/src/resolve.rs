use crate::{data_frame_utils, Error, PointCloud, PointDataColumnNames};
use chrono::{TimeZone, Utc};
use ecoord::{FrameId, ReferenceFrames, TransformId};
use nalgebra::Point3;

use polars::prelude::{concat, DataFrame, IntoLazy, LazyFrame, TakeRandom};

use rayon::prelude::*;

impl PointCloud {
    pub fn resolve_to_frame(&mut self, target_frame_id: FrameId) -> Result<(), Error> {
        // interesting: https://stackoverflow.com/a/65287197
        let mut partitioned: Vec<DataFrame> = self.point_data.clone().partition_by([
            PointDataColumnNames::FrameId.as_str(),
            PointDataColumnNames::TimestampSeconds.as_str(),
            PointDataColumnNames::TimestampNanoSeconds.as_str(),
        ])?;

        partitioned.sort_by_key(|k| {
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
        });

        let partitioned: Vec<DataFrame> = partitioned
            .iter()
            .map(|df| resolve_data_frame(df, &self.reference_frames, &target_frame_id).unwrap())
            .collect();
        // dbg!(&partitioned);

        let partitioned_lazy: Vec<LazyFrame> =
            partitioned.iter().map(|d| d.clone().lazy()).collect();

        let merged_again = concat(partitioned_lazy, true, true)
            .unwrap()
            .collect()
            .unwrap();

        self.point_data = merged_again;

        //.lazy()
        //.groupby([
        //    PointDataColumnNames::FrameId.as_str(),
        //    PointDataColumnNames::TimestampSeconds.as_str(),
        //    PointDataColumnNames::TimestampNanoSeconds.as_str(),
        //])
        //.agg([count(), col(PointDataColumnNames::X.as_str())])
        //.limit(50)
        //.collect()?;

        //println!("{}", df);

        /*let points = self.get_all_points();
        let timestamps = self.get_all_timesamps().unwrap();
        let frame_ids = self.get_frame_ids().unwrap();

        //points.iter().zip(timestamps).zip(frame_ids).par_iter.map(|((p, t), f)| t.0.0)

        let transformed_points: Vec<Point3<f64>> = points
            .par_iter()
            .zip(timestamps.par_iter())
            .zip(frame_ids.par_iter())
            .map(|((p, t), f)| {
                let transform_id = TransformId::new(target_frame_id.clone(), f.clone());

                let isometry = self
                    .reference_frames
                    .derive_transform_graph(&None, &Some(*t))
                    .get_isometry(&transform_id);

                isometry * p
            })
            .collect();

        self.update_points(transformed_points, target_frame_id)?;*/

        Ok(())
    }
}

/// Resolves points to target_frame_id
///
/// Expects a data frame with only one
fn resolve_data_frame(
    point_data: &DataFrame,
    reference_frame: &ReferenceFrames,
    target_frame_id: &FrameId,
) -> Result<DataFrame, Error> {
    let timestamp_seconds = point_data
        .column(PointDataColumnNames::TimestampSeconds.as_str())
        .unwrap()
        .i64()
        .unwrap()
        .get(0)
        .unwrap();
    let timestamp_nanoseconds = point_data
        .column(PointDataColumnNames::TimestampNanoSeconds.as_str())
        .unwrap()
        .u32()
        .unwrap()
        .get(0)
        .unwrap();
    let t = Utc
        .timestamp_opt(timestamp_seconds, timestamp_nanoseconds)
        .unwrap();

    let frame_id = data_frame_utils::extract_frame_ids(point_data)
        .unwrap()
        .first()
        .unwrap()
        .clone();
    let transform_id = TransformId::new(target_frame_id.clone(), frame_id);

    let graph = reference_frame.derive_transform_graph(&None, &Some(t));
    let isometry = graph.get_isometry(&transform_id);

    // println!("{:?}", frame_id);
    //println!("{:?}.{:?}", timestamp_seconds, timestamp_nanoseconds);
    //let temp_transform_id = TransformId::new(FrameId::from("slam_map"), FrameId::from("base_link"));
    //let temp_isometry = graph.get_isometry(&temp_transform_id);
    //println!("{:?}", temp_isometry);
    //println!("");

    let all_points = data_frame_utils::extract_points(point_data);

    let transformed_points: Vec<Point3<f64>> =
        all_points.par_iter().map(|p| isometry * p).collect();

    let updated_data_frame = data_frame_utils::update_points(point_data, transformed_points)?;
    Ok(updated_data_frame)
}
