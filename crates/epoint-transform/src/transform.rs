use ecoord::{FrameId, TransformId};
use epoint_core::{PointCloud, PointCloudInfo};

use chrono::{DateTime, Utc};
use epoint_core::PointDataColumnNames;
use nalgebra::{Point3, Vector3};
use polars::prelude::{NamedFrom, Series};
use rayon::prelude::*;

pub fn translate(point_cloud: &PointCloud, translation: Vector3<f64>) -> PointCloud {
    let mut translated_data = point_cloud.point_data().clone();
    translated_data
        .apply(PointDataColumnNames::X.as_str(), |x| x + translation.x)
        .expect("TODO: panic message");
    translated_data
        .apply(PointDataColumnNames::Y.as_str(), |y| y + translation.y)
        .expect("TODO: panic message");
    translated_data
        .apply(PointDataColumnNames::Z.as_str(), |z| z + translation.z)
        .expect("TODO: panic message");

    let info = point_cloud.info().clone();
    let frames = point_cloud.reference_frames().clone();

    PointCloud::from_data_frame(translated_data, info, frames).unwrap()
}

pub fn transform_to_frame(
    point_cloud: &PointCloud,
    timestamp: Option<DateTime<Utc>>,
    target_frame_id: &FrameId,
) -> PointCloud {
    let point_cloud_frame_id = point_cloud
        .frame_id()
        .expect("Point cloud must reference a frame id.");

    let transform_id = TransformId::new(target_frame_id.clone(), point_cloud_frame_id.clone());

    let isometry = point_cloud
        .reference_frames()
        .derive_transform_graph(&None, &timestamp)
        .get_isometry(&transform_id);

    let transformed_points: Vec<Point3<f64>> = point_cloud
        .get_all_points()
        .par_iter()
        .map(|p| isometry * p)
        .collect();

    let x_vector: Vec<f64> = transformed_points.par_iter().map(|p| p.x).collect();
    let y_vector: Vec<f64> = transformed_points.par_iter().map(|p| p.y).collect();
    let z_vector: Vec<f64> = transformed_points.par_iter().map(|p| p.z).collect();

    let x_series: Series = Series::new(PointDataColumnNames::X.as_str(), &x_vector);
    let y_series: Series = Series::new(PointDataColumnNames::Y.as_str(), &y_vector);
    let z_series: Series = Series::new(PointDataColumnNames::Z.as_str(), &z_vector);

    let mut transformed_point_data = point_cloud.point_data().clone();
    transformed_point_data
        .replace(PointDataColumnNames::X.as_str(), x_series)
        .expect("TODO: panic message");
    transformed_point_data
        .replace(PointDataColumnNames::Y.as_str(), y_series)
        .expect("TODO: panic message");
    transformed_point_data
        .replace(PointDataColumnNames::Z.as_str(), z_series)
        .expect("TODO: panic message");

    let transformed_info = PointCloudInfo::new(Some(target_frame_id.clone()));

    PointCloud::from_data_frame(
        transformed_point_data,
        transformed_info,
        point_cloud.reference_frames().clone(),
    )
    .unwrap()
}
