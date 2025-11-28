use crate::e57::error::Error;
use crate::e57::error::Error::{NoPointCloudsInFile, NotSupported};
use e57::{CartesianCoordinate, PointCloudReaderSimple};
use ecoord::{FrameId, StaticTransform, Transform, TransformEdge, TransformTree};
use epoint_core::{PointCloud, PointCloudInfo, PointDataColumnType};
use epoint_transform::merge;
use nalgebra::{Quaternion, UnitQuaternion, Vector3};
use polars::frame::DataFrame;
use polars::prelude::{Column, NamedFrom};
use std::io::{BufReader, Read, Seek};

pub fn import_point_cloud_from_e57_file<R: Read + Seek>(
    reader: R,
    reference_frame_id: FrameId,
    sensor_frame_id: FrameId,
) -> Result<PointCloud, Error> {
    let mut e57_reader = e57::E57Reader::new(BufReader::new(reader))?;
    if e57_reader.pointclouds().is_empty() {
        return Err(NoPointCloudsInFile());
    }
    if e57_reader.pointclouds().len() > 1 {
        return Err(NotSupported(
            "reading e57 file with multiple point clouds is not supported",
        ));
    }

    let mut point_clouds: Vec<PointCloud> = Vec::new();
    for (current_index, current_e57_point_cloud) in e57_reader.pointclouds().into_iter().enumerate()
    {
        let mut e57_point_cloud_reader = e57_reader.pointcloud_simple(&current_e57_point_cloud)?;
        e57_point_cloud_reader.apply_pose(false);

        if current_e57_point_cloud.acquisition_start.is_some()
            || current_e57_point_cloud.acquisition_end.is_some()
        {
            return Err(NotSupported(
                "times acquisition_start and acquisition_end are not yet supported",
            ));
        }

        let point_cloud = import_individual_point_cloud_from_e57_file(
            e57_point_cloud_reader,
            &current_e57_point_cloud.transform,
            &reference_frame_id,
            &sensor_frame_id,
            current_e57_point_cloud.has_timestamp(),
            current_e57_point_cloud.has_intensity(),
            current_e57_point_cloud.has_color(),
        )?;

        point_clouds.push(point_cloud);
    }

    let merged_point_cloud = merge(point_clouds)?;
    Ok(merged_point_cloud)
}

pub fn import_individual_point_cloud_from_e57_file<T: Read + Seek>(
    e57_point_cloud_reader: PointCloudReaderSimple<T>,
    transform: &Option<e57::Transform>,
    reference_frame_id: &FrameId,
    sensor_frame_id: &FrameId,
    has_timestamp_column: bool,
    has_intensity_column: bool,
    has_color_columns: bool,
) -> Result<PointCloud, Error> {
    if has_timestamp_column {
        return Err(NotSupported("timestamp column is not supported"));
    }

    let mut x_values: Vec<f64> = Vec::new();
    let mut y_values: Vec<f64> = Vec::new();
    let mut z_values: Vec<f64> = Vec::new();
    let mut intensity_values: Vec<f32> = Vec::new();
    let mut color_red_values: Vec<u16> = Vec::new();
    let mut color_green_values: Vec<u16> = Vec::new();
    let mut color_blue_values: Vec<u16> = Vec::new();

    for current_e57_point in e57_point_cloud_reader.flatten() {
        // check if point contains complete information
        match current_e57_point.cartesian {
            CartesianCoordinate::Valid { .. } => {}
            CartesianCoordinate::Direction { .. } => continue,
            CartesianCoordinate::Invalid => continue,
        }
        if has_intensity_column && current_e57_point.intensity.is_none() {
            continue;
        }

        // parse point
        if let CartesianCoordinate::Valid { x, y, z } = current_e57_point.cartesian {
            x_values.push(x);
            y_values.push(y);
            z_values.push(z);
        }
        if let Some(intensity) = current_e57_point.intensity {
            intensity_values.push(intensity);
        }
        if let Some(color) = current_e57_point.color {
            color_red_values.push((color.red * u16::MAX as f32) as u16);
            color_green_values.push((color.green * u16::MAX as f32) as u16);
            color_blue_values.push((color.blue * u16::MAX as f32) as u16);
        }
    }

    let mut point_data_columns = vec![
        Column::new(PointDataColumnType::X.into(), x_values),
        Column::new(PointDataColumnType::Y.into(), y_values),
        Column::new(PointDataColumnType::Z.into(), z_values),
    ];

    if has_intensity_column {
        point_data_columns.push(Column::new(
            PointDataColumnType::Intensity.into(),
            intensity_values,
        ));
    }
    if has_color_columns {
        point_data_columns.push(Column::new(
            PointDataColumnType::ColorRed.into(),
            color_red_values,
        ));
        point_data_columns.push(Column::new(
            PointDataColumnType::ColorGreen.into(),
            color_green_values,
        ));
        point_data_columns.push(Column::new(
            PointDataColumnType::ColorBlue.into(),
            color_blue_values,
        ));
    }

    let point_data = DataFrame::new(point_data_columns).expect("should work");
    let transform_tree = parse_transform_tree(transform, reference_frame_id, sensor_frame_id);
    let point_cloud_info = PointCloudInfo::new(Some(sensor_frame_id.clone()));

    let point_cloud = PointCloud::from_data_frame(point_data, point_cloud_info, transform_tree)?;

    Ok(point_cloud)
}

// see also: http://www.libe57.org/bestCoordinates.html
fn parse_transform_tree(
    transform: &Option<e57::Transform>,
    reference_frame_id: &FrameId,
    sensor_frame_id: &FrameId,
) -> TransformTree {
    let Some(transform) = transform else {
        return TransformTree::default();
    };

    let translation: Vector3<f64> = convert_translation(&transform.translation);
    let rotation = convert_rotation(&transform.rotation);

    let static_transform = StaticTransform::new(
        reference_frame_id.clone(),
        sensor_frame_id.clone(),
        Transform::new(translation, rotation),
    );

    TransformTree::new(vec![TransformEdge::Static(static_transform)], Vec::new())
        .expect("should work")
}

fn convert_translation(value: &e57::Translation) -> Vector3<f64> {
    Vector3::new(value.x, value.y, value.z)
}

fn convert_rotation(value: &e57::Quaternion) -> UnitQuaternion<f64> {
    UnitQuaternion::new_unchecked(Quaternion::new(value.w, value.x, value.y, value.z))
}
