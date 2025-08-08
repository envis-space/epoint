use crate::e57::error::Error;
use crate::e57::error::Error::{
    NoAcquisitionTimeSpecified, NoPointCloudsInFile, NotMatchingNumberOfAcquisitionTimes,
    NotSupported,
};
use chrono::{Timelike, Utc};
use e57::{CartesianCoordinate, PointCloudReaderSimple};
use ecoord::{
    ChannelId, ExtrapolationMethod, FrameId, InterpolationMethod, ReferenceFrames, Transform,
    TransformId, TransformInfo,
};
use epoint_core::{PointCloud, PointCloudInfo, PointDataColumnType};
use epoint_transform::merge;
use nalgebra::{Quaternion, UnitQuaternion, Vector3};
use polars::frame::DataFrame;
use polars::prelude::{Column, NamedFrom};
use std::collections::HashMap;
use std::io::{BufReader, Read, Seek};

pub fn import_point_cloud_from_e57_file<R: Read + Seek>(
    reader: R,
    acquisition_start_timestamps: Option<Vec<chrono::DateTime<Utc>>>,
    channel_id: ChannelId,
    world_frame_id: FrameId,
    scanner_frame_id: FrameId,
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
    if let Some(acquisition_start_timestamps) = &acquisition_start_timestamps
        && acquisition_start_timestamps.len() != e57_reader.pointclouds().len()
    {
        return Err(NotMatchingNumberOfAcquisitionTimes {
            set_acquisition_times: acquisition_start_timestamps.len(),
            point_clouds: e57_reader.pointclouds().len(),
        });
    }

    let mut point_clouds: Vec<PointCloud> = Vec::new();
    for (current_index, current_e57_point_cloud) in e57_reader.pointclouds().into_iter().enumerate()
    {
        let mut e57_point_cloud_reader = e57_reader
            .pointcloud_simple(&current_e57_point_cloud)
            .unwrap();
        e57_point_cloud_reader.apply_pose(false);

        if current_e57_point_cloud.acquisition_start.is_some()
            || current_e57_point_cloud.acquisition_end.is_some()
        {
            return Err(NotSupported(
                "times acquisition_start and acquisition_end are not yet supported",
            ));
        }
        let current_acquisition_start_timestamp = acquisition_start_timestamps
            .as_ref()
            .map(|x| x[current_index]);

        let point_cloud = import_individual_point_cloud_from_e57_file(
            e57_point_cloud_reader,
            &current_e57_point_cloud.transform,
            &channel_id,
            &world_frame_id,
            &scanner_frame_id,
            current_acquisition_start_timestamp,
            None,
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
    channel_id: &ChannelId,
    world_frame_id: &FrameId,
    scanner_frame_id: &FrameId,
    acquisition_start_timestamp: Option<chrono::DateTime<Utc>>,
    acquisition_end_timestamp: Option<chrono::DateTime<Utc>>,
    has_timestamp_column: bool,
    has_intensity_column: bool,
    has_color_columns: bool,
) -> Result<PointCloud, Error> {
    let acquisition_start_timestamp =
        acquisition_start_timestamp.ok_or(NoAcquisitionTimeSpecified())?;
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
    let number_of_points = x_values.len();

    let mut point_data_columns = vec![
        Column::new(PointDataColumnType::X.into(), x_values),
        Column::new(PointDataColumnType::Y.into(), y_values),
        Column::new(PointDataColumnType::Z.into(), z_values),
        Column::new(
            PointDataColumnType::TimestampSeconds.into(),
            vec![acquisition_start_timestamp.timestamp(); number_of_points],
        ),
        Column::new(
            PointDataColumnType::TimestampNanoSeconds.into(),
            vec![acquisition_start_timestamp.nanosecond(); number_of_points],
        ),
        // TODO: only create XYZIJKW columns if really needed
        Column::new(
            PointDataColumnType::SensorTranslationX.into(),
            vec![0.0f64; number_of_points],
        ),
        Column::new(
            PointDataColumnType::SensorTranslationY.into(),
            vec![0.0f64; number_of_points],
        ),
        Column::new(
            PointDataColumnType::SensorTranslationZ.into(),
            vec![0.0f64; number_of_points],
        ),
        Column::new(
            PointDataColumnType::SensorRotationI.into(),
            vec![0.0f64; number_of_points],
        ),
        Column::new(
            PointDataColumnType::SensorRotationJ.into(),
            vec![0.0f64; number_of_points],
        ),
        Column::new(
            PointDataColumnType::SensorRotationK.into(),
            vec![0.0f64; number_of_points],
        ),
        Column::new(
            PointDataColumnType::SensorRotationW.into(),
            vec![1.0f64; number_of_points],
        ),
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
    let reference_frames = parse_reference_frame(
        acquisition_start_timestamp,
        transform,
        channel_id,
        world_frame_id,
        scanner_frame_id,
    );
    let point_cloud_info = PointCloudInfo::new(Some(scanner_frame_id.clone()));

    let point_cloud = PointCloud::from_data_frame(point_data, point_cloud_info, reference_frames)?;

    Ok(point_cloud)
}

// see also: http://www.libe57.org/bestCoordinates.html
fn parse_reference_frame(
    timestamp: chrono::DateTime<Utc>,
    transform: &Option<e57::Transform>,
    channel_id: &ChannelId,
    world_frame_id: &FrameId,
    scanner_frame_id: &FrameId,
) -> ReferenceFrames {
    let Some(transform) = transform else {
        return ReferenceFrames::default();
    };

    let transform_id = TransformId::new(world_frame_id.clone(), scanner_frame_id.clone());

    let translation = convert_translation(&transform.translation);
    let rotation = convert_rotation(&transform.rotation);
    let transform = Transform::new(timestamp, translation, rotation);
    let mut transforms: HashMap<(ChannelId, TransformId), Vec<Transform>> = HashMap::new();
    transforms.insert((channel_id.clone(), transform_id.clone()), vec![transform]);

    let mut transform_info: HashMap<TransformId, TransformInfo> = HashMap::new();
    transform_info.insert(
        transform_id,
        TransformInfo::new(InterpolationMethod::Step, ExtrapolationMethod::Constant),
    );

    ReferenceFrames::new(transforms, HashMap::new(), HashMap::new(), transform_info)
        .expect("should work")
}

fn convert_translation(value: &e57::Translation) -> Vector3<f64> {
    Vector3::new(value.x, value.y, value.z)
}

fn convert_rotation(value: &e57::Quaternion) -> UnitQuaternion<f64> {
    UnitQuaternion::new_unchecked(Quaternion::new(value.w, value.x, value.y, value.z))
}

//pub fn convert_date_time(date_time: e57::DateTime) -> chrono::DateTime<Utc> {
//}
