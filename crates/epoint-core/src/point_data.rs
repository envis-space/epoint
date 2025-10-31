use crate::Error;
use crate::Error::{
    ColumnAlreadyExists, LowerBoundEqualsUpperBound, LowerBoundExceedsUpperBound, NoData,
    ObligatoryColumn, ShapeMismatch, TypeMismatch,
};
use chrono::{DateTime, TimeZone, Utc};
use ecoord::octree::OctantIndex;
use ecoord::{AxisAlignedBoundingBox, FrameId, ReferenceFrames, SphericalPoint3, TransformId};
use nalgebra::{Isometry3, Point3, Quaternion, UnitQuaternion};
use palette::Srgb;
use parry3d_f64::shape::ConvexPolyhedron;
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::HashSet;
use std::ops::{Add, Sub};
use std::str::FromStr;

const COLUMN_NAME_X_STR: &str = "x";
const COLUMN_NAME_Y_STR: &str = "y";
const COLUMN_NAME_Z_STR: &str = "z";
const COLUMN_NAME_ID_STR: &str = "id";
const COLUMN_NAME_FRAME_ID_STR: &str = "frame_id";
const COLUMN_NAME_TIMESTAMP_SEC_STR: &str = "timestamp_sec";
const COLUMN_NAME_TIMESTAMP_NANOSEC_STR: &str = "timestamp_nanosec";
const COLUMN_NAME_INTENSITY_STR: &str = "intensity";
const COLUMN_NAME_SENSOR_TRANSLATION_X_STR: &str = "sensor_translation_x";
const COLUMN_NAME_SENSOR_TRANSLATION_Y_STR: &str = "sensor_translation_y";
const COLUMN_NAME_SENSOR_TRANSLATION_Z_STR: &str = "sensor_translation_z";
const COLUMN_NAME_SENSOR_ROTATION_X_STR: &str = "sensor_rotation_x";
const COLUMN_NAME_SENSOR_ROTATION_Y_STR: &str = "sensor_rotation_y";
const COLUMN_NAME_SENSOR_ROTATION_Z_STR: &str = "sensor_rotation_z";
const COLUMN_NAME_SENSOR_ROTATION_W_STR: &str = "sensor_rotation_w";
const COLUMN_NAME_COLOR_RED_STR: &str = "color_red";
const COLUMN_NAME_COLOR_GREEN_STR: &str = "color_green";
const COLUMN_NAME_COLOR_BLUE_STR: &str = "color_blue";
const COLUMN_NAME_SPHERICAL_AZIMUTH_STR: &str = "spherical_azimuth";
const COLUMN_NAME_SPHERICAL_ELEVATION_STR: &str = "spherical_elevation";
const COLUMN_NAME_SPHERICAL_RANGE_STR: &str = "spherical_range";
const COLUMN_NAME_OCTANT_INDEX_LEVEL_STR: &str = "octant_index_level";
const COLUMN_NAME_OCTANT_INDEX_X_STR: &str = "octant_index_x";
const COLUMN_NAME_OCTANT_INDEX_Y_STR: &str = "octant_index_y";
const COLUMN_NAME_OCTANT_INDEX_Z_STR: &str = "octant_index_z";
const COLUMN_NAME_POINT_SOURCE_ID_STR: &str = "point_source_id";

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PointDataColumnType {
    /// X coordinate (mandatory)
    X,
    /// Y coordinate (mandatory)
    Y,
    /// Z coordinate (mandatory)
    Z,
    /// Identifier for an individual point (optional)
    Id,
    /// Coordinate frame the point is defined in (optional)
    FrameId,
    /// UNIX timestamp: non-leap seconds since January 1, 1970 0:00:00 UTC (optional)
    TimestampSecond,
    /// Nanoseconds since the last whole non-leap second
    TimestampNanoSecond,
    /// Representation of the pulse return magnitude
    Intensity,
    /// Sensor translation X coordinate
    SensorTranslationX,
    /// Sensor translation Y coordinate
    SensorTranslationY,
    /// Sensor translation Z coordinate
    SensorTranslationZ,
    /// Sensor rotation X coordinate
    SensorRotationX,
    /// Sensor rotation Y coordinate
    SensorRotationY,
    /// Sensor rotation Z coordinate
    SensorRotationZ,
    /// Sensor rotation W coordinate
    SensorRotationW,
    /// Red image channel value
    ColorRed,
    /// Green image channel value
    ColorGreen,
    /// Blue image channel value
    ColorBlue,
    /// Azimuth in the context of spherical coordinates
    SphericalAzimuth,
    /// Elevation in the context of spherical coordinates
    SphericalElevation,
    /// Range in the context of spherical coordinates
    SphericalRange,
    /// Level of octant index
    OctantIndexLevel,
    /// X index of octant
    OctantIndexX,
    /// Y index of octant
    OctantIndexY,
    /// Z index of octant
    OctantIndexZ,
    /// Indicates the source from which this point originated (e.g., flight line, sortie number, route number, or setup identifier)
    /// Valid values: 1-65,535; zero is reserved.
    PointSourceId,
}

impl std::str::FromStr for PointDataColumnType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            COLUMN_NAME_X_STR => Ok(PointDataColumnType::X),
            COLUMN_NAME_Y_STR => Ok(PointDataColumnType::Y),
            COLUMN_NAME_Z_STR => Ok(PointDataColumnType::Z),
            COLUMN_NAME_ID_STR => Ok(PointDataColumnType::Id),
            COLUMN_NAME_FRAME_ID_STR => Ok(PointDataColumnType::FrameId),
            COLUMN_NAME_TIMESTAMP_SEC_STR => Ok(PointDataColumnType::TimestampSecond),
            COLUMN_NAME_TIMESTAMP_NANOSEC_STR => Ok(PointDataColumnType::TimestampNanoSecond),
            COLUMN_NAME_INTENSITY_STR => Ok(PointDataColumnType::Intensity),
            COLUMN_NAME_SENSOR_TRANSLATION_X_STR => Ok(PointDataColumnType::SensorTranslationX),
            COLUMN_NAME_SENSOR_TRANSLATION_Y_STR => Ok(PointDataColumnType::SensorTranslationY),
            COLUMN_NAME_SENSOR_TRANSLATION_Z_STR => Ok(PointDataColumnType::SensorTranslationZ),
            COLUMN_NAME_COLOR_RED_STR => Ok(PointDataColumnType::ColorRed),
            COLUMN_NAME_COLOR_GREEN_STR => Ok(PointDataColumnType::ColorGreen),
            COLUMN_NAME_COLOR_BLUE_STR => Ok(PointDataColumnType::ColorBlue),
            COLUMN_NAME_SPHERICAL_AZIMUTH_STR => Ok(PointDataColumnType::SphericalAzimuth),
            COLUMN_NAME_SPHERICAL_ELEVATION_STR => Ok(PointDataColumnType::SphericalElevation),
            COLUMN_NAME_SPHERICAL_RANGE_STR => Ok(PointDataColumnType::SphericalRange),
            COLUMN_NAME_POINT_SOURCE_ID_STR => Ok(PointDataColumnType::PointSourceId),
            _ => Err(()),
        }
    }
}

impl PointDataColumnType {
    pub fn as_str(&self) -> &'static str {
        match self {
            PointDataColumnType::X => COLUMN_NAME_X_STR,
            PointDataColumnType::Y => COLUMN_NAME_Y_STR,
            PointDataColumnType::Z => COLUMN_NAME_Z_STR,
            PointDataColumnType::Id => COLUMN_NAME_ID_STR,
            PointDataColumnType::FrameId => COLUMN_NAME_FRAME_ID_STR,
            PointDataColumnType::TimestampSecond => COLUMN_NAME_TIMESTAMP_SEC_STR,
            PointDataColumnType::TimestampNanoSecond => COLUMN_NAME_TIMESTAMP_NANOSEC_STR,
            PointDataColumnType::Intensity => COLUMN_NAME_INTENSITY_STR,
            PointDataColumnType::SensorTranslationX => COLUMN_NAME_SENSOR_TRANSLATION_X_STR,
            PointDataColumnType::SensorTranslationY => COLUMN_NAME_SENSOR_TRANSLATION_Y_STR,
            PointDataColumnType::SensorTranslationZ => COLUMN_NAME_SENSOR_TRANSLATION_Z_STR,
            PointDataColumnType::SensorRotationX => COLUMN_NAME_SENSOR_ROTATION_X_STR,
            PointDataColumnType::SensorRotationY => COLUMN_NAME_SENSOR_ROTATION_Y_STR,
            PointDataColumnType::SensorRotationZ => COLUMN_NAME_SENSOR_ROTATION_Z_STR,
            PointDataColumnType::SensorRotationW => COLUMN_NAME_SENSOR_ROTATION_W_STR,
            PointDataColumnType::ColorRed => COLUMN_NAME_COLOR_RED_STR,
            PointDataColumnType::ColorGreen => COLUMN_NAME_COLOR_GREEN_STR,
            PointDataColumnType::ColorBlue => COLUMN_NAME_COLOR_BLUE_STR,
            PointDataColumnType::SphericalAzimuth => COLUMN_NAME_SPHERICAL_AZIMUTH_STR,
            PointDataColumnType::SphericalElevation => COLUMN_NAME_SPHERICAL_ELEVATION_STR,
            PointDataColumnType::SphericalRange => COLUMN_NAME_SPHERICAL_RANGE_STR,
            PointDataColumnType::OctantIndexLevel => COLUMN_NAME_OCTANT_INDEX_LEVEL_STR,
            PointDataColumnType::OctantIndexX => COLUMN_NAME_OCTANT_INDEX_X_STR,
            PointDataColumnType::OctantIndexY => COLUMN_NAME_OCTANT_INDEX_Y_STR,
            PointDataColumnType::OctantIndexZ => COLUMN_NAME_OCTANT_INDEX_Z_STR,
            PointDataColumnType::PointSourceId => COLUMN_NAME_POINT_SOURCE_ID_STR,
        }
    }

    pub fn data_frame_data_type(&self) -> DataType {
        match self {
            PointDataColumnType::X => DataType::Float64,
            PointDataColumnType::Y => DataType::Float64,
            PointDataColumnType::Z => DataType::Float64,
            PointDataColumnType::Id => DataType::UInt64,
            PointDataColumnType::FrameId => DataType::Categorical(
                Categories::new(
                    "frame_ids".into(),
                    "ecoord_frames".into(),
                    CategoricalPhysical::U8,
                ),
                Arc::new(CategoricalMapping::new(u8::MAX as usize)),
            ),
            PointDataColumnType::TimestampSecond => DataType::Int64,
            PointDataColumnType::TimestampNanoSecond => DataType::UInt32,
            PointDataColumnType::Intensity => DataType::Float32,
            PointDataColumnType::SensorTranslationX => DataType::Float64,
            PointDataColumnType::SensorTranslationY => DataType::Float64,
            PointDataColumnType::SensorTranslationZ => DataType::Float64,
            PointDataColumnType::SensorRotationX => DataType::Float64,
            PointDataColumnType::SensorRotationY => DataType::Float64,
            PointDataColumnType::SensorRotationZ => DataType::Float64,
            PointDataColumnType::SensorRotationW => DataType::Float64,
            PointDataColumnType::ColorRed => DataType::UInt16,
            PointDataColumnType::ColorGreen => DataType::UInt16,
            PointDataColumnType::ColorBlue => DataType::UInt16,
            PointDataColumnType::SphericalAzimuth => DataType::Float64,
            PointDataColumnType::SphericalElevation => DataType::Float64,
            PointDataColumnType::SphericalRange => DataType::Float64,
            PointDataColumnType::OctantIndexLevel => DataType::UInt32,
            PointDataColumnType::OctantIndexX => DataType::UInt64,
            PointDataColumnType::OctantIndexY => DataType::UInt64,
            PointDataColumnType::OctantIndexZ => DataType::UInt64,
            PointDataColumnType::PointSourceId => DataType::UInt16,
        }
    }
}

impl From<PointDataColumnType> for PlSmallStr {
    fn from(value: PointDataColumnType) -> Self {
        value.as_str().into()
    }
}

/// Wrapper around the data frame with type-safe columns.
#[derive(Debug, Clone, PartialEq)]
pub struct PointData {
    pub data_frame: DataFrame,
}

impl PointData {
    pub fn new(data_frame: DataFrame) -> Result<Self, Error> {
        if data_frame.is_empty() {
            return Err(NoData("point_data"));
        }

        // check if column types are correct
        let data_frame_column_types: Vec<PointDataColumnType> = data_frame
            .get_column_names()
            .iter()
            .filter_map(|x| PointDataColumnType::from_str(x).ok())
            .collect();

        for current_column_type in data_frame_column_types {
            let current_column = data_frame
                .column(current_column_type.as_str())
                .expect("column must exist");
            if current_column.dtype() != &current_column_type.data_frame_data_type() {
                return Err(TypeMismatch {
                    column: current_column_type.as_str(),
                    expected: current_column_type.data_frame_data_type().to_string(),
                    actual: current_column.dtype().to_string(),
                });
            }
        }

        Ok(Self { data_frame })
    }

    pub fn new_unchecked(data_frame: DataFrame) -> Self {
        Self { data_frame }
    }

    pub fn height(&self) -> usize {
        self.data_frame.height()
    }

    pub fn is_empty(&self) -> bool {
        self.data_frame.is_empty()
    }
}

impl PointData {
    pub fn get_x_values(&self) -> &Float64Chunked {
        self.data_frame
            .column(PointDataColumnType::X.as_str())
            .expect("column mandatory")
            .f64()
            .expect("type must be f64")
    }
    pub fn get_y_values(&self) -> &Float64Chunked {
        self.data_frame
            .column(PointDataColumnType::Y.as_str())
            .expect("column mandatory")
            .f64()
            .expect("type must be f64")
    }
    pub fn get_z_values(&self) -> &Float64Chunked {
        self.data_frame
            .column(PointDataColumnType::Z.as_str())
            .expect("column mandatory")
            .f64()
            .expect("type must be f64")
    }

    pub fn get_id_values(&self) -> Result<&UInt64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::Id.as_str())?
            .u64()
            .expect("type must be u64");
        Ok(values)
    }

    pub fn get_frame_id_values(&self) -> Result<&Categorical8Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::FrameId.as_str())?
            .cat8()
            .expect("type must be categorical");
        Ok(values)
    }

    pub fn get_timestamp_sec_values(&self) -> Result<&Int64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::TimestampSecond.as_str())?
            .i64()
            .expect("type must be i64");
        Ok(values)
    }

    pub fn get_timestamp_nanosec_values(&self) -> Result<&UInt32Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::TimestampNanoSecond.as_str())?
            .u32()
            .expect("type must be u32");
        Ok(values)
    }

    pub fn get_intensity_values(&self) -> Result<&Float32Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::Intensity.as_str())?
            .f32()
            .expect("type must be f32");
        Ok(values)
    }

    pub fn get_sensor_translation_x_values(&self) -> Result<&Float64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::SensorTranslationX.as_str())?
            .f64()
            .expect("type must be f64");
        Ok(values)
    }

    pub fn get_sensor_translation_y_values(&self) -> Result<&Float64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::SensorTranslationY.as_str())?
            .f64()
            .expect("type must be f64");
        Ok(values)
    }

    pub fn get_sensor_translation_z_values(&self) -> Result<&Float64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::SensorTranslationZ.as_str())?
            .f64()
            .expect("type must be f64");
        Ok(values)
    }

    pub fn get_sensor_rotation_x_values(&self) -> Result<&Float64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::SensorRotationX.as_str())?
            .f64()
            .expect("type must be f64");
        Ok(values)
    }

    pub fn get_sensor_rotation_y_values(&self) -> Result<&Float64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::SensorRotationY.as_str())?
            .f64()
            .expect("type must be f64");
        Ok(values)
    }

    pub fn get_sensor_rotation_z_values(&self) -> Result<&Float64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::SensorRotationZ.as_str())?
            .f64()
            .expect("type must be f64");
        Ok(values)
    }

    pub fn get_sensor_rotation_w_values(&self) -> Result<&Float64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::SensorRotationW.as_str())?
            .f64()
            .expect("type must be f64");
        Ok(values)
    }

    pub fn get_color_red_values(&self) -> Result<&UInt16Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::ColorRed.as_str())?
            .u16()
            .expect("type must be u16");
        Ok(values)
    }

    pub fn get_color_green_values(&self) -> Result<&UInt16Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::ColorGreen.as_str())?
            .u16()
            .expect("type must be u16");
        Ok(values)
    }

    pub fn get_color_blue_values(&self) -> Result<&UInt16Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::ColorBlue.as_str())?
            .u16()
            .expect("type must be u16");
        Ok(values)
    }

    pub fn get_spherical_azimuth_values(&self) -> Result<&Float64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::SphericalAzimuth.as_str())?
            .f64()
            .expect("type must be f64");
        Ok(values)
    }

    pub fn get_spherical_elevation_values(&self) -> Result<&Float64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::SphericalElevation.as_str())?
            .f64()
            .expect("type must be f64");
        Ok(values)
    }

    pub fn get_spherical_range_values(&self) -> Result<&Float64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::SphericalRange.as_str())?
            .f64()
            .expect("type must be f64");
        Ok(values)
    }

    pub fn get_point_source_id_values(&self) -> Result<&UInt16Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::PointSourceId.as_str())?
            .u16()
            .expect("type must be f64");
        Ok(values)
    }
}

impl PointData {
    pub fn contains_id_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::Id.as_str())
            .is_ok()
    }

    pub fn contains_frame_id_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::FrameId.as_str())
            .is_ok()
    }

    pub fn contains_timestamp_sec_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::TimestampSecond.as_str())
            .is_ok()
    }

    pub fn contains_timestamp_nanosec_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::TimestampNanoSecond.as_str())
            .is_ok()
    }

    pub fn contains_intensity_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::Intensity.as_str())
            .is_ok()
    }

    pub fn contains_sensor_translation_x_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::SensorTranslationX.as_str())
            .is_ok()
    }

    pub fn contains_sensor_translation_y_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::SensorTranslationY.as_str())
            .is_ok()
    }

    pub fn contains_sensor_translation_z_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::SensorTranslationZ.as_str())
            .is_ok()
    }

    pub fn contains_sensor_rotation_x_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::SensorRotationX.as_str())
            .is_ok()
    }

    pub fn contains_sensor_rotation_y_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::SensorRotationY.as_str())
            .is_ok()
    }

    pub fn contains_sensor_rotation_z_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::SensorRotationZ.as_str())
            .is_ok()
    }

    pub fn contains_sensor_rotation_w_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::SensorRotationW.as_str())
            .is_ok()
    }

    pub fn contains_color_red_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::ColorRed.as_str())
            .is_ok()
    }

    pub fn contains_color_green_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::ColorGreen.as_str())
            .is_ok()
    }

    pub fn contains_color_blue_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::ColorBlue.as_str())
            .is_ok()
    }

    pub fn contains_spherical_azimuth_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::SphericalAzimuth.as_str())
            .is_ok()
    }

    pub fn contains_spherical_elevation_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::SphericalElevation.as_str())
            .is_ok()
    }

    pub fn contains_spherical_range_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::SphericalRange.as_str())
            .is_ok()
    }

    pub fn contains_point_source_id_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::PointSourceId.as_str())
            .is_ok()
    }

    pub fn contains_octant_index_level_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::OctantIndexLevel.as_str())
            .is_ok()
    }

    pub fn contains_octant_index_x_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::OctantIndexX.as_str())
            .is_ok()
    }

    pub fn contains_octant_index_y_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::OctantIndexY.as_str())
            .is_ok()
    }

    pub fn contains_octant_index_z_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::OctantIndexZ.as_str())
            .is_ok()
    }
}

impl PointData {
    pub fn contains_timestamps(&self) -> bool {
        self.contains_timestamp_sec_column() && self.contains_timestamp_nanosec_column()
    }

    pub fn contains_sensor_translation(&self) -> bool {
        self.contains_sensor_translation_x_column()
            && self.contains_sensor_translation_y_column()
            && self.contains_sensor_translation_z_column()
    }

    pub fn contains_sensor_rotation(&self) -> bool {
        self.contains_sensor_rotation_x_column()
            && self.contains_sensor_rotation_y_column()
            && self.contains_sensor_rotation_z_column()
            && self.contains_sensor_rotation_w_column()
    }

    pub fn contains_sensor_pose(&self) -> bool {
        self.contains_sensor_translation() && self.contains_sensor_rotation()
    }

    pub fn contains_colors(&self) -> bool {
        self.contains_color_red_column()
            && self.contains_color_green_column()
            && self.contains_color_blue_column()
    }

    pub fn contains_octant_indices(&self) -> bool {
        self.contains_octant_index_level_column()
            && self.contains_octant_index_x_column()
            && self.contains_octant_index_y_column()
            && self.contains_octant_index_z_column()
    }
}

impl PointData {
    /// Returns all points as a vector in the local coordinate frame.
    pub fn get_all_points(&self) -> Vec<Point3<f64>> {
        let x_values = self.get_x_values();
        let y_values = self.get_y_values();
        let z_values = self.get_z_values();

        let all_points: Vec<Point3<f64>> = (0..self.data_frame.height())
            .into_par_iter()
            .map(|i: usize| {
                Point3::new(
                    x_values.get(i).unwrap(),
                    y_values.get(i).unwrap(),
                    z_values.get(i).unwrap(),
                )
            })
            .collect();

        all_points
    }

    pub fn get_all_frame_ids(&self) -> Result<Vec<FrameId>, Error> {
        let values = self
            .get_frame_id_values()?
            .cast(&DataType::String)?
            .str()?
            .into_no_null_iter()
            .map(|f| f.to_string().into())
            .collect();
        Ok(values)
    }

    pub fn get_all_timestamps(&self) -> Result<Vec<DateTime<Utc>>, Error> {
        let timestamp_sec_series = self.get_timestamp_sec_values()?;
        let timestamp_nanosec_series = self.get_timestamp_nanosec_values()?;

        let timestamps: Vec<DateTime<Utc>> = timestamp_sec_series
            .into_iter()
            .zip(timestamp_nanosec_series)
            .map(|(current_sec, current_nanosec)| {
                Utc.timestamp_opt(current_sec.unwrap(), current_nanosec.unwrap())
                    .unwrap()
            })
            .collect();
        Ok(timestamps)
    }

    /// Returns all sensor translations as points in the local coordinate frame.
    pub fn get_all_sensor_translations(&self) -> Result<Vec<Point3<f64>>, Error> {
        let x_values = self.get_sensor_translation_x_values()?;
        let y_values = self.get_sensor_translation_y_values()?;
        let z_values = self.get_sensor_translation_z_values()?;

        let all_sensor_translations: Vec<Point3<f64>> = (0..self.data_frame.height())
            .into_par_iter()
            .map(|i: usize| {
                Point3::new(
                    x_values.get(i).unwrap(),
                    y_values.get(i).unwrap(),
                    z_values.get(i).unwrap(),
                )
            })
            .collect();

        Ok(all_sensor_translations)
    }

    /// Returns all sensor rotations as quaternions in the local coordinate frame.
    pub fn get_all_sensor_rotations(&self) -> Result<Vec<UnitQuaternion<f64>>, Error> {
        let i_values = self.get_sensor_rotation_x_values()?;
        let j_values = self.get_sensor_rotation_y_values()?;
        let k_values = self.get_sensor_rotation_z_values()?;
        let w_values = self.get_sensor_rotation_w_values()?;

        let all_sensor_rotations: Vec<UnitQuaternion<f64>> = (0..self.data_frame.height())
            .into_par_iter()
            .map(|i: usize| {
                UnitQuaternion::new_unchecked(Quaternion::new(
                    i_values.get(i).unwrap(),
                    j_values.get(i).unwrap(),
                    k_values.get(i).unwrap(),
                    w_values.get(i).unwrap(),
                ))
            })
            .collect();

        Ok(all_sensor_rotations)
    }

    /// Returns all sensor rotations as quaternions in the local coordinate frame.
    pub fn get_all_sensor_poses(&self) -> Result<Vec<Isometry3<f64>>, Error> {
        let sensor_translations = self.get_all_sensor_translations()?;
        let sensor_rotations = self.get_all_sensor_rotations()?;

        let all_sensor_poses: Vec<Isometry3<f64>> = (0..self.data_frame.height())
            .into_par_iter()
            .map(|i: usize| {
                Isometry3::from_parts(
                    (*sensor_translations.get(i).unwrap()).into(),
                    *sensor_rotations.get(i).unwrap(),
                )
            })
            .collect();

        Ok(all_sensor_poses)
    }

    pub fn get_all_colors(&self) -> Result<Vec<Srgb<u16>>, Error> {
        let red_color_values = self.get_color_red_values()?;
        let green_color_values = self.get_color_green_values()?;
        let blue_color_values = self.get_color_blue_values()?;

        let all_colors: Vec<Srgb<u16>> = (0..self.data_frame.height())
            .into_par_iter()
            .map(|i: usize| {
                Srgb::new(
                    red_color_values.get(i).unwrap(),
                    green_color_values.get(i).unwrap(),
                    blue_color_values.get(i).unwrap(),
                )
            })
            .collect();

        Ok(all_colors)
    }

    pub fn get_all_spherical_points(&self) -> Result<Vec<SphericalPoint3<f64>>, Error> {
        let range_values = self.get_spherical_range_values()?;
        let elevation_values = self.get_spherical_elevation_values()?;
        let azimuth_values = self.get_spherical_azimuth_values()?;

        let all_spherical_points: Vec<SphericalPoint3<f64>> = (0..self.data_frame.height())
            .into_par_iter()
            .map(|i: usize| {
                SphericalPoint3::new(
                    range_values.get(i).unwrap(),
                    elevation_values.get(i).unwrap(),
                    azimuth_values.get(i).unwrap(),
                )
            })
            .collect();

        Ok(all_spherical_points)
    }
}

impl PointData {
    pub fn remove_colors(&mut self) -> Result<(), Error> {
        self.data_frame = self.data_frame.drop_many([
            PointDataColumnType::ColorRed.as_str(),
            PointDataColumnType::ColorGreen.as_str(),
            PointDataColumnType::ColorBlue.as_str(),
        ]);

        Ok(())
    }
}

impl PointData {
    pub fn get_distinct_frame_ids(&self) -> Result<HashSet<FrameId>, Error> {
        let values: HashSet<FrameId> = self
            .data_frame
            .column(PointDataColumnType::FrameId.as_str())?
            .unique()?
            .cat8()
            .expect("type must be categorical")
            .cast(&DataType::String)
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .map(|f| f.to_string().into())
            .collect();

        Ok(values)
    }

    pub fn get_timestamp_min(&self) -> Result<Option<DateTime<Utc>>, Error> {
        let all_timestamps = self.get_all_timestamps()?;
        let value = all_timestamps.iter().min();
        Ok(value.copied())
    }

    pub fn get_timestamp_max(&self) -> Result<Option<DateTime<Utc>>, Error> {
        let all_timestamps = self.get_all_timestamps()?;
        let value = all_timestamps.iter().max();
        Ok(value.copied())
    }

    pub fn get_median_time(&self) -> Result<DateTime<Utc>, Error> {
        let mut all_time = self.get_all_timestamps()?;
        all_time.sort();
        let mid = all_time.len() / 2;
        Ok(all_time[mid])
    }

    /// Returns the [AABB](https://en.wikipedia.org/wiki/Minimum_bounding_box#Axis-aligned_minimum_bounding_box).
    pub fn get_axis_aligned_bounding_box(&self) -> AxisAlignedBoundingBox {
        let min_bound = self.get_local_min();
        let max_bound = self.get_local_max();

        AxisAlignedBoundingBox::new(min_bound, max_bound).expect("should work")
    }

    /// Returns the minimum point of the [AABB](https://en.wikipedia.org/wiki/Minimum_bounding_box#Axis-aligned_minimum_bounding_box).
    pub fn get_local_min(&self) -> Point3<f64> {
        let x = self.get_x_values().min().expect("point cloud not empty");
        let y = self.get_y_values().min().expect("point cloud not empty");
        let z = self.get_z_values().min().expect("point cloud not empty");
        Point3::new(x, y, z)
    }

    /// Returns the maximum point of the [AABB](https://en.wikipedia.org/wiki/Minimum_bounding_box#Axis-aligned_minimum_bounding_box).
    pub fn get_local_max(&self) -> Point3<f64> {
        let x = self.get_x_values().max().expect("point cloud not empty");
        let y = self.get_y_values().max().expect("point cloud not empty");
        let z = self.get_z_values().max().expect("point cloud not empty");
        Point3::new(x, y, z)
    }

    /// Returns the center point of the [AABB](https://en.wikipedia.org/wiki/Minimum_bounding_box#Axis-aligned_minimum_bounding_box).
    pub fn get_local_center(&self) -> Point3<f64> {
        let local_min = self.get_local_min();
        let diagonal = self.get_local_max() - local_min;
        local_min + diagonal / 2.0
    }

    /// Returns the minimum sensor translation point of the [AABB](https://en.wikipedia.org/wiki/Minimum_bounding_box#Axis-aligned_minimum_bounding_box).
    pub fn get_local_sensor_translation_min(&self) -> Result<Point3<f64>, Error> {
        let x = self
            .get_sensor_translation_x_values()?
            .min()
            .expect("point cloud not empty");
        let y = self
            .get_sensor_translation_y_values()?
            .min()
            .expect("point cloud not empty");
        let z = self
            .get_sensor_translation_z_values()?
            .min()
            .expect("point cloud not empty");
        Ok(Point3::new(x, y, z))
    }

    /// Returns the maximum sensor translation point of the [AABB](https://en.wikipedia.org/wiki/Minimum_bounding_box#Axis-aligned_minimum_bounding_box).
    pub fn get_local_sensor_translation_max(&self) -> Result<Point3<f64>, Error> {
        let x = self
            .get_sensor_translation_x_values()?
            .max()
            .expect("point cloud not empty");
        let y = self
            .get_sensor_translation_y_values()?
            .max()
            .expect("point cloud not empty");
        let z = self
            .get_sensor_translation_z_values()?
            .max()
            .expect("point cloud not empty");
        Ok(Point3::new(x, y, z))
    }

    pub fn get_id_min(&self) -> Result<Option<u64>, Error> {
        let value = self.get_id_values()?.min();
        Ok(value)
    }

    pub fn get_id_max(&self) -> Result<Option<u64>, Error> {
        let value = self.get_id_values()?.max();
        Ok(value)
    }

    pub fn get_intensity_min(&self) -> Result<Option<f32>, Error> {
        let value = self.get_intensity_values()?.min();
        Ok(value)
    }
    pub fn get_intensity_max(&self) -> Result<Option<f32>, Error> {
        let value = self.get_intensity_values()?.max();
        Ok(value)
    }

    pub fn derive_convex_hull(&self) -> Option<ConvexPolyhedron> {
        let points: Vec<parry3d_f64::math::Point<f64>> = self
            .get_all_points()
            .iter()
            .map(|p| parry3d_f64::math::Point::new(p.x, p.y, p.z))
            .collect();
        ConvexPolyhedron::from_convex_hull(&points)
    }
}

impl PointData {
    /// Adds a sequentially increasing id column, if no column exists.
    pub fn add_sequential_id(&mut self) -> Result<(), Error> {
        if self.contains_id_column() {
            return Err(ColumnAlreadyExists(PointDataColumnType::Id.as_str()));
        }

        let values: Vec<u64> = Vec::from_iter(0u64..self.data_frame.height() as u64);
        if values.len() != self.data_frame.height() {
            return Err(ShapeMismatch("should have the same height"));
        }

        let new_series = Series::new(PointDataColumnType::Id.into(), values);
        self.data_frame.with_column(new_series)?;

        Ok(())
    }

    /// Derives spherical points.
    pub fn derive_spherical_points(&mut self) -> Result<(), Error> {
        let spherical_points: Vec<SphericalPoint3<f64>> = self
            .get_all_points()
            .into_par_iter()
            .map(|p| p.into())
            .collect();

        self.add_spherical_points(spherical_points)?;

        Ok(())
    }

    /// Add a new column to this DataFrame or replace an existing one.
    pub fn add_i64_column(&mut self, name: &str, values: Vec<i64>) -> Result<(), Error> {
        if values.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "values have a different length than point_data",
            ));
        }

        let new_series = Series::new(name.into(), values);
        self.data_frame.with_column(new_series)?;
        Ok(())
    }

    /// Add a new u16 column to this DataFrame or replace an existing one.
    pub fn add_u16_column(&mut self, name: &str, values: Vec<u16>) -> Result<(), Error> {
        if values.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "values have a different length than point_data",
            ));
        }

        let new_series = Series::new(name.into(), values);
        self.data_frame.with_column(new_series)?;
        Ok(())
    }

    /// Add a new u32 column to this DataFrame or replace an existing one.
    pub fn add_u32_column(&mut self, name: &str, values: Vec<u32>) -> Result<(), Error> {
        if values.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "values have a different length than point_data",
            ));
        }

        let new_series = Series::new(name.into(), values);
        self.data_frame.with_column(new_series)?;
        Ok(())
    }

    /// Add a new u64 column to this DataFrame or replace an existing one.
    pub fn add_u64_column(&mut self, name: &str, values: Vec<u64>) -> Result<(), Error> {
        if values.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "values have a different length than point_data",
            ));
        }

        let new_series = Series::new(name.into(), values);
        self.data_frame.with_column(new_series)?;
        Ok(())
    }

    /// Add a new column to this DataFrame or replace an existing one.
    pub fn add_f32_column(&mut self, name: &str, values: Vec<f32>) -> Result<(), Error> {
        if values.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "values have a different length than point_data",
            ));
        }

        let new_series = Series::new(name.into(), values);
        self.data_frame.with_column(new_series)?;
        Ok(())
    }

    /// Add a new f64 column to this DataFrame or replace an existing one.
    pub fn add_f64_column(&mut self, name: &str, values: Vec<f64>) -> Result<(), Error> {
        if values.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "values have a different length than point_data",
            ));
        }

        let new_series = Series::new(name.into(), values);
        self.data_frame.with_column(new_series)?;
        Ok(())
    }

    /// Removes a column from the point cloud.
    pub fn remove_column(&mut self, column: &str) -> Result<(), Error> {
        if column == PointDataColumnType::X.as_str()
            || column == PointDataColumnType::Y.as_str()
            || column == PointDataColumnType::Z.as_str()
        {
            return Err(ObligatoryColumn);
        }
        self.data_frame = self.data_frame.drop(column)?;

        Ok(())
    }
}

impl PointData {
    pub fn update_points_in_place(&mut self, points: Vec<Point3<f64>>) -> Result<(), Error> {
        if points.len() != self.data_frame.height() {
            return Err(ShapeMismatch("points"));
        }

        if self
            .data_frame
            .column(PointDataColumnType::FrameId.as_str())
            .is_ok()
        {
            let _ = self
                .data_frame
                .drop_in_place(PointDataColumnType::FrameId.as_str())
                .expect("Column should be successfully replaced");
        }

        let x_series = Series::new(
            PointDataColumnType::X.into(),
            points.iter().map(|p| p.x).collect::<Vec<f64>>(),
        );
        let y_series = Series::new(
            PointDataColumnType::Y.into(),
            points.iter().map(|p| p.y).collect::<Vec<f64>>(),
        );
        let z_series = Series::new(
            PointDataColumnType::Z.into(),
            points.iter().map(|p| p.z).collect::<Vec<f64>>(),
        );
        self.data_frame
            .replace(PointDataColumnType::X.as_str(), x_series)?;
        self.data_frame
            .replace(PointDataColumnType::Y.as_str(), y_series)?;
        self.data_frame
            .replace(PointDataColumnType::Z.as_str(), z_series)?;

        Ok(())
    }

    // pub fn derive_spherical_points_in_place(&mut self) -> Result<(), Error> {}

    pub fn update_sensor_translations_in_place(
        &mut self,
        sensor_translations: Vec<Point3<f64>>,
    ) -> Result<(), Error> {
        if sensor_translations.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "sensor_translations has a different size than the point_data",
            ));
        }

        let sensor_translation_x_series = Series::new(
            PointDataColumnType::SensorTranslationX.into(),
            sensor_translations
                .iter()
                .map(|p| p.x)
                .collect::<Vec<f64>>(),
        );
        let sensor_translation_y_series = Series::new(
            PointDataColumnType::SensorTranslationY.into(),
            sensor_translations
                .iter()
                .map(|p| p.y)
                .collect::<Vec<f64>>(),
        );
        let sensor_translation_z_series = Series::new(
            PointDataColumnType::SensorTranslationZ.into(),
            sensor_translations
                .iter()
                .map(|p| p.z)
                .collect::<Vec<f64>>(),
        );
        self.data_frame.replace(
            PointDataColumnType::SensorTranslationX.as_str(),
            sensor_translation_x_series,
        )?;
        self.data_frame.replace(
            PointDataColumnType::SensorTranslationY.as_str(),
            sensor_translation_y_series,
        )?;
        self.data_frame.replace(
            PointDataColumnType::SensorTranslationZ.as_str(),
            sensor_translation_z_series,
        )?;

        Ok(())
    }

    pub fn update_sensor_rotations_in_place(
        &mut self,
        sensor_rotations: Vec<UnitQuaternion<f64>>,
    ) -> Result<(), Error> {
        if sensor_rotations.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "sensor_translations has a different size than the point_data",
            ));
        }

        let sensor_rotation_x_series = Series::new(
            PointDataColumnType::SensorRotationX.into(),
            sensor_rotations.iter().map(|r| r.i).collect::<Vec<f64>>(),
        );
        let sensor_rotation_y_series = Series::new(
            PointDataColumnType::SensorRotationY.into(),
            sensor_rotations.iter().map(|r| r.j).collect::<Vec<f64>>(),
        );
        let sensor_rotation_z_series = Series::new(
            PointDataColumnType::SensorRotationZ.into(),
            sensor_rotations.iter().map(|r| r.k).collect::<Vec<f64>>(),
        );
        let sensor_rotation_w_series = Series::new(
            PointDataColumnType::SensorRotationW.into(),
            sensor_rotations.iter().map(|r| r.w).collect::<Vec<f64>>(),
        );

        self.data_frame.replace(
            PointDataColumnType::SensorRotationX.as_str(),
            sensor_rotation_x_series,
        )?;
        self.data_frame.replace(
            PointDataColumnType::SensorRotationY.as_str(),
            sensor_rotation_y_series,
        )?;
        self.data_frame.replace(
            PointDataColumnType::SensorRotationZ.as_str(),
            sensor_rotation_z_series,
        )?;
        self.data_frame.replace(
            PointDataColumnType::SensorRotationW.as_str(),
            sensor_rotation_w_series,
        )?;

        Ok(())
    }

    pub fn update_sensor_poses_in_place(
        &mut self,
        sensor_poses: Vec<Isometry3<f64>>,
    ) -> Result<(), Error> {
        if sensor_poses.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "sensor_poses has a different size than the point_data",
            ));
        }

        let sensor_translations: Vec<Point3<f64>> = sensor_poses
            .iter()
            .map(|i| i.translation.vector.into())
            .collect();
        self.update_sensor_translations_in_place(sensor_translations)?;

        let sensor_rotations: Vec<UnitQuaternion<f64>> =
            sensor_poses.into_iter().map(|i| i.rotation).collect();
        self.update_sensor_rotations_in_place(sensor_rotations)?;

        Ok(())
    }

    pub fn add_spherical_points(
        &mut self,
        spherical_points: Vec<SphericalPoint3<f64>>,
    ) -> Result<(), Error> {
        if spherical_points.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "spherical_points has a different size than the point_data",
            ));
        }

        let spherical_azimuth_series = Series::new(
            PointDataColumnType::SphericalAzimuth.into(),
            spherical_points.iter().map(|p| p.phi).collect::<Vec<f64>>(),
        );
        let spherical_elevation_series = Series::new(
            PointDataColumnType::SphericalElevation.into(),
            spherical_points
                .iter()
                .map(|p| p.theta)
                .collect::<Vec<f64>>(),
        );
        let spherical_range_series = Series::new(
            PointDataColumnType::SphericalRange.into(),
            spherical_points.iter().map(|p| p.r).collect::<Vec<f64>>(),
        );
        self.data_frame.with_column(spherical_azimuth_series)?;
        self.data_frame.with_column(spherical_elevation_series)?;
        self.data_frame.with_column(spherical_range_series)?;

        Ok(())
    }

    pub fn add_octant_indices(&mut self, octant_indices: Vec<OctantIndex>) -> Result<(), Error> {
        if octant_indices.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "octant_indices has a different size than the point_data",
            ));
        }

        let octant_index_level_series = Series::new(
            PointDataColumnType::OctantIndexLevel.into(),
            octant_indices.iter().map(|i| i.level).collect::<Vec<u32>>(),
        );
        let octant_index_x_series = Series::new(
            PointDataColumnType::OctantIndexX.into(),
            octant_indices.iter().map(|i| i.x).collect::<Vec<u64>>(),
        );
        let octant_index_y_series = Series::new(
            PointDataColumnType::OctantIndexY.into(),
            octant_indices.iter().map(|i| i.y).collect::<Vec<u64>>(),
        );
        let octant_index_z_series = Series::new(
            PointDataColumnType::OctantIndexZ.into(),
            octant_indices.iter().map(|i| i.z).collect::<Vec<u64>>(),
        );

        self.data_frame.with_column(octant_index_level_series)?;
        self.data_frame.with_column(octant_index_x_series)?;
        self.data_frame.with_column(octant_index_y_series)?;
        self.data_frame.with_column(octant_index_z_series)?;

        Ok(())
    }

    pub fn add_unique_frame_id(&mut self, frame_id: FrameId) -> Result<(), Error> {
        let frame_ids = vec![frame_id; self.data_frame.height()];
        self.add_frame_ids(frame_ids)?;

        Ok(())
    }

    pub fn add_frame_ids(&mut self, frame_ids: Vec<FrameId>) -> Result<(), Error> {
        if self.contains_frame_id_column() {
            return Err(ColumnAlreadyExists(PointDataColumnType::FrameId.as_str()));
        };
        if frame_ids.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "frame_ids has a different size than the point_data",
            ));
        }

        let frame_id_series = Series::new(
            PointDataColumnType::FrameId.into(),
            frame_ids
                .into_iter()
                .map(|x| x.to_string())
                .collect::<Vec<String>>(),
        )
        .cast(&PointDataColumnType::FrameId.data_frame_data_type())
        .unwrap();
        self.data_frame.with_column(frame_id_series)?;

        Ok(())
    }

    pub fn add_unique_sensor_translation(
        &mut self,
        sensor_translation: Point3<f64>,
    ) -> Result<(), Error> {
        let sensor_translations: Vec<Point3<f64>> =
            vec![sensor_translation; self.data_frame.height()];
        self.add_sensor_translations(sensor_translations)?;

        Ok(())
    }

    pub fn add_sensor_translations(
        &mut self,
        sensor_translations: Vec<Point3<f64>>,
    ) -> Result<(), Error> {
        if sensor_translations.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "sensor_translation has a different size than the point_data",
            ));
        }

        let sensor_translation_x_series = Series::new(
            PointDataColumnType::SensorTranslationX.into(),
            sensor_translations
                .iter()
                .map(|p| p.x)
                .collect::<Vec<f64>>(),
        );
        let sensor_translation_y_series = Series::new(
            PointDataColumnType::SensorTranslationY.into(),
            sensor_translations
                .iter()
                .map(|p| p.y)
                .collect::<Vec<f64>>(),
        );
        let sensor_translation_z_series = Series::new(
            PointDataColumnType::SensorTranslationZ.into(),
            sensor_translations
                .iter()
                .map(|p| p.z)
                .collect::<Vec<f64>>(),
        );
        self.data_frame.with_column(sensor_translation_x_series)?;
        self.data_frame.with_column(sensor_translation_y_series)?;
        self.data_frame.with_column(sensor_translation_z_series)?;

        Ok(())
    }

    pub fn add_unique_sensor_rotation(
        &mut self,
        sensor_rotation: UnitQuaternion<f64>,
    ) -> Result<(), Error> {
        let sensor_rotations: Vec<UnitQuaternion<f64>> =
            vec![sensor_rotation; self.data_frame.height()];
        self.add_sensor_rotations(sensor_rotations)?;

        Ok(())
    }

    pub fn add_sensor_rotations(
        &mut self,
        sensor_rotations: Vec<UnitQuaternion<f64>>,
    ) -> Result<(), Error> {
        if sensor_rotations.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "sensor_rotations has a different size than the point_data",
            ));
        }

        let sensor_rotation_x_series = Series::new(
            PointDataColumnType::SensorRotationX.into(),
            sensor_rotations.iter().map(|r| r.i).collect::<Vec<f64>>(),
        );
        let sensor_rotation_y_series = Series::new(
            PointDataColumnType::SensorRotationY.into(),
            sensor_rotations.iter().map(|r| r.j).collect::<Vec<f64>>(),
        );
        let sensor_rotation_z_series = Series::new(
            PointDataColumnType::SensorRotationZ.into(),
            sensor_rotations.iter().map(|r| r.k).collect::<Vec<f64>>(),
        );
        let sensor_rotation_w_series = Series::new(
            PointDataColumnType::SensorRotationW.into(),
            sensor_rotations.iter().map(|r| r.w).collect::<Vec<f64>>(),
        );
        self.data_frame.with_column(sensor_rotation_x_series)?;
        self.data_frame.with_column(sensor_rotation_y_series)?;
        self.data_frame.with_column(sensor_rotation_z_series)?;
        self.data_frame.with_column(sensor_rotation_w_series)?;

        Ok(())
    }

    pub fn add_unique_sensor_pose(&mut self, sensor_pose: Isometry3<f64>) -> Result<(), Error> {
        let sensor_poses: Vec<Isometry3<f64>> = vec![sensor_pose; self.data_frame.height()];
        self.add_sensor_poses(sensor_poses)?;

        Ok(())
    }

    pub fn add_sensor_poses(&mut self, sensor_poses: Vec<Isometry3<f64>>) -> Result<(), Error> {
        if sensor_poses.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "sensor_rotations has a different size than the point_data",
            ));
        }

        let sensor_translations: Vec<Point3<f64>> = sensor_poses
            .iter()
            .map(|i| i.translation.vector.into())
            .collect();
        self.add_sensor_translations(sensor_translations)?;

        let sensor_rotations: Vec<UnitQuaternion<f64>> =
            sensor_poses.into_iter().map(|i| i.rotation).collect();
        self.add_sensor_rotations(sensor_rotations)?;

        Ok(())
    }

    pub fn add_unique_color(&mut self, color: palette::Srgb<u16>) -> Result<(), Error> {
        let colors = vec![color; self.data_frame.height()];
        self.add_colors(colors)?;

        Ok(())
    }

    pub fn add_colors(&mut self, colors: Vec<palette::Srgb<u16>>) -> Result<(), Error> {
        if colors.len() != self.data_frame.height() {
            return Err(ShapeMismatch(
                "colors has a different size than the point_data",
            ));
        }

        let color_red_series = Series::new(
            PointDataColumnType::ColorRed.into(),
            colors.iter().map(|p| p.red).collect::<Vec<u16>>(),
        );
        let color_green_series = Series::new(
            PointDataColumnType::ColorGreen.into(),
            colors.iter().map(|p| p.green).collect::<Vec<u16>>(),
        );
        let color_blue_series = Series::new(
            PointDataColumnType::ColorBlue.into(),
            colors.iter().map(|p| p.blue).collect::<Vec<u16>>(),
        );
        self.data_frame.with_column(color_red_series)?;
        self.data_frame.with_column(color_green_series)?;
        self.data_frame.with_column(color_blue_series)?;

        Ok(())
    }

    pub fn filter_by_row_indices(&self, row_indices: HashSet<usize>) -> Result<PointData, Error> {
        if row_indices.is_empty() {
            return Err(Error::NoRowIndices);
        }
        let row_index_max = row_indices.iter().max().unwrap();
        if self.data_frame.height() < *row_index_max {
            return Err(Error::RowIndexOutsideRange);
        }

        let boolean_mask: BooleanChunked = (0..self.data_frame.height())
            .into_par_iter()
            .map(|x| row_indices.contains(&x))
            .collect();
        let filtered_data_frame = self.data_frame.filter(&boolean_mask)?;
        Ok(PointData::new_unchecked(filtered_data_frame))
    }

    pub fn filter_by_boolean_mask(
        &self,
        boolean_mask: &BooleanChunked,
    ) -> Result<PointData, Error> {
        if self.data_frame.height() < boolean_mask.len() {
            return Err(Error::RowIndexOutsideRange);
        }

        let filtered_data_frame = self.data_frame.filter(boolean_mask)?;
        Ok(PointData::new_unchecked(filtered_data_frame))
    }

    pub fn filter_by_bounds(
        &self,
        bound_min: Point3<f64>,
        bound_max: Point3<f64>,
    ) -> Result<Option<PointData>, Error> {
        let filtered_data_frame = self
            .data_frame
            .clone()
            .lazy()
            .filter(
                col(PointDataColumnType::X.as_str())
                    .gt_eq(bound_min.x)
                    .and(col(PointDataColumnType::X.as_str()).lt_eq(bound_max.x))
                    .and(col(PointDataColumnType::Y.as_str()).gt_eq(bound_min.y))
                    .and(col(PointDataColumnType::Y.as_str()).lt_eq(bound_max.y))
                    .and(col(PointDataColumnType::Z.as_str()).gt_eq(bound_min.z))
                    .and(col(PointDataColumnType::Z.as_str()).lt_eq(bound_max.z)),
            )
            .collect()?;

        if filtered_data_frame.height() == 0 {
            return Ok(None);
        }

        Ok(Some(PointData::new_unchecked(filtered_data_frame)))
    }

    pub fn filter_by_beam_length(
        &self,
        beam_length_min: f64,
        beam_length_max: f64,
    ) -> Result<Option<PointData>, Error> {
        if beam_length_min > beam_length_max {
            return Err(LowerBoundExceedsUpperBound);
        }
        if beam_length_min == beam_length_max {
            return Err(LowerBoundEqualsUpperBound);
        }
        if !self.contains_sensor_translation() {
            return Err(Error::NoSensorTranslationColumn);
        }

        let filtered_data_frame = self
            .data_frame
            .clone()
            .lazy()
            .filter(
                col(PointDataColumnType::X.as_str())
                    .sub(col(PointDataColumnType::SensorTranslationX.as_str()))
                    .pow(2)
                    .add(
                        col(PointDataColumnType::Y.as_str())
                            .sub(col(PointDataColumnType::SensorTranslationY.as_str()))
                            .pow(2),
                    )
                    .add(
                        col(PointDataColumnType::Z.as_str())
                            .sub(col(PointDataColumnType::SensorTranslationZ.as_str()))
                            .pow(2),
                    )
                    .is_between(
                        beam_length_min * beam_length_min,
                        beam_length_max * beam_length_max,
                        ClosedInterval::Both,
                    ),
            )
            .collect()?;

        if filtered_data_frame.height() == 0 {
            return Ok(None);
        }

        Ok(Some(PointData::new_unchecked(filtered_data_frame)))
    }

    pub fn filter_by_x_min(&self, x_min: f64) -> Result<Option<PointData>, Error> {
        let filtered_data_frame = self
            .data_frame
            .clone()
            .lazy()
            .filter(col(PointDataColumnType::X.as_str()).gt_eq(x_min))
            .collect()?;

        if filtered_data_frame.height() == 0 {
            return Ok(None);
        }

        Ok(Some(PointData::new_unchecked(filtered_data_frame)))
    }

    pub fn filter_by_x_max(&self, x_max: f64) -> Result<Option<PointData>, Error> {
        let filtered_data_frame = self
            .data_frame
            .clone()
            .lazy()
            .filter(col(PointDataColumnType::X.as_str()).lt_eq(lit(x_max)))
            .collect()?;

        if filtered_data_frame.height() == 0 {
            return Ok(None);
        }

        Ok(Some(PointData::new_unchecked(filtered_data_frame)))
    }

    pub fn filter_by_y_min(&self, y_min: f64) -> Result<Option<PointData>, Error> {
        let filtered_data_frame = self
            .data_frame
            .clone()
            .lazy()
            .filter(col(PointDataColumnType::Y.as_str()).gt_eq(y_min))
            .collect()?;

        if filtered_data_frame.height() == 0 {
            return Ok(None);
        }

        Ok(Some(PointData::new_unchecked(filtered_data_frame)))
    }

    pub fn filter_by_y_max(&self, y_max: f64) -> Result<Option<PointData>, Error> {
        let filtered_data_frame = self
            .data_frame
            .clone()
            .lazy()
            .filter(col(PointDataColumnType::Y.as_str()).lt_eq(lit(y_max)))
            .collect()?;

        if filtered_data_frame.height() == 0 {
            return Ok(None);
        }

        Ok(Some(PointData::new_unchecked(filtered_data_frame)))
    }

    pub fn filter_by_z_min(&self, z_min: f64) -> Result<Option<PointData>, Error> {
        let filtered_data_frame = self
            .data_frame
            .clone()
            .lazy()
            .filter(col(PointDataColumnType::Z.as_str()).gt_eq(z_min))
            .collect()?;

        if filtered_data_frame.height() == 0 {
            return Ok(None);
        }

        Ok(Some(PointData::new_unchecked(filtered_data_frame)))
    }

    pub fn filter_by_z_max(&self, z_max: f64) -> Result<Option<PointData>, Error> {
        let filtered_data_frame = self
            .data_frame
            .clone()
            .lazy()
            .filter(col(PointDataColumnType::Z.as_str()).lt_eq(lit(z_max)))
            .collect()?;

        if filtered_data_frame.height() == 0 {
            return Ok(None);
        }

        Ok(Some(PointData::new_unchecked(filtered_data_frame)))
    }

    pub fn filter_by_spherical_range_min(
        &self,
        spherical_range_min: f64,
    ) -> Result<Option<PointData>, Error> {
        if !self.contains_spherical_range_column() {
            return Err(Error::NoSphericalRangeColumn);
        }

        let filtered_data_frame = self
            .data_frame
            .clone()
            .lazy()
            .filter(col(PointDataColumnType::SphericalRange.as_str()).gt_eq(spherical_range_min))
            .collect()?;

        if filtered_data_frame.height() == 0 {
            return Ok(None);
        }

        Ok(Some(PointData::new_unchecked(filtered_data_frame)))
    }

    pub fn filter_by_spherical_range_max(
        &self,
        spherical_range_max: f64,
    ) -> Result<Option<PointData>, Error> {
        if !self.contains_spherical_range_column() {
            return Err(Error::NoSphericalRangeColumn);
        }

        let filtered_data_frame = self
            .data_frame
            .clone()
            .lazy()
            .filter(
                col(PointDataColumnType::SphericalRange.as_str()).lt_eq(lit(spherical_range_max)),
            )
            .collect()?;

        if filtered_data_frame.height() == 0 {
            return Ok(None);
        }

        Ok(Some(PointData::new_unchecked(filtered_data_frame)))
    }

    pub fn filter_by_octant_index(&self, index: OctantIndex) -> Result<Option<PointData>, Error> {
        if !self.contains_octant_indices() {
            return Err(Error::NoOctantIndicesColumns);
        }

        let filtered_data_frame = self
            .data_frame
            .clone()
            .lazy()
            .filter(
                col(PointDataColumnType::OctantIndexLevel.as_str())
                    .eq(lit(index.level))
                    .and(col(PointDataColumnType::OctantIndexX.as_str()).eq(lit(index.x)))
                    .and(col(PointDataColumnType::OctantIndexY.as_str()).eq(lit(index.y)))
                    .and(col(PointDataColumnType::OctantIndexZ.as_str()).eq(lit(index.z))),
            )
            .collect()?;

        if filtered_data_frame.height() == 0 {
            return Ok(None);
        }

        Ok(Some(PointData::new_unchecked(filtered_data_frame)))
    }
}

impl PointData {
    /// Resolves points to target_frame_id
    ///
    /// Expects a data frame with only one
    pub fn resolve_data_frame(
        &mut self,
        reference_frame: &ReferenceFrames,
        timestamp: &Option<DateTime<Utc>>,
        frame_id: &FrameId,
        target_frame_id: &FrameId,
    ) -> Result<(), Error> {
        let transform_id = TransformId::new(target_frame_id.clone(), frame_id.clone());

        let graph = reference_frame.derive_transform_graph(&None, timestamp)?;
        let isometry = graph.get_isometry(&transform_id)?;

        let transformed_points: Vec<Point3<f64>> = self
            .get_all_points()
            .par_iter()
            .map(|p| isometry * p)
            .collect();
        self.update_points_in_place(transformed_points)?;

        if let Ok(all_sensor_translations) = &self.get_all_sensor_translations() {
            let transformed_sensor_translations: Vec<Point3<f64>> = all_sensor_translations
                .par_iter()
                .map(|p| isometry * p)
                .collect();
            self.update_sensor_translations_in_place(transformed_sensor_translations)?;
        }

        if let Ok(all_sensor_rotations) = &self.get_all_sensor_rotations() {
            let transformed_sensor_rotations: Vec<UnitQuaternion<f64>> = all_sensor_rotations
                .par_iter()
                .map(|r| isometry.rotation * r)
                .collect();
            self.update_sensor_rotations_in_place(transformed_sensor_rotations)?;
        }

        Ok(())
    }
}
