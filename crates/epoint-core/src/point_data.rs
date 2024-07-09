use crate::bounding_box::AxisAlignedBoundingBox;
use crate::Error;
use crate::Error::{
    ColumnAlreadyExists, ColumnNameMisMatch, NoData, ObligatoryColumn, ShapeMisMatch, TypeMisMatch,
};
use chrono::{DateTime, TimeZone, Utc};
use ecoord::{FrameId, ReferenceFrames, SphericalPoint3, TransformId};
use nalgebra::Point3;
use palette::Srgb;
use parry3d_f64::shape::ConvexPolyhedron;
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::HashSet;
use std::ops::{Add, Sub};
use std::str::FromStr;
use strum_macros::EnumIter;

const COLUMN_NAME_X_STR: &str = "x";
const COLUMN_NAME_Y_STR: &str = "y";
const COLUMN_NAME_Z_STR: &str = "z";
const COLUMN_NAME_ID_STR: &str = "id";
const COLUMN_NAME_FRAME_ID_STR: &str = "frame_id";
const COLUMN_NAME_TIMESTAMP_SEC_STR: &str = "timestamp_sec";
const COLUMN_NAME_TIMESTAMP_NANOSEC_STR: &str = "timestamp_nanosec";
const COLUMN_NAME_INTENSITY_STR: &str = "intensity";
const COLUMN_NAME_BEAM_ORIGIN_X_STR: &str = "beam_origin_x";
const COLUMN_NAME_BEAM_ORIGIN_Y_STR: &str = "beam_origin_y";
const COLUMN_NAME_BEAM_ORIGIN_Z_STR: &str = "beam_origin_z";
const COLUMN_NAME_COLOR_RED_STR: &str = "color_red";
const COLUMN_NAME_COLOR_GREEN_STR: &str = "color_green";
const COLUMN_NAME_COLOR_BLUE_STR: &str = "color_blue";
const COLUMN_NAME_SPHERICAL_AZIMUTH_STR: &str = "spherical_azimuth";
const COLUMN_NAME_SPHERICAL_ELEVATION_STR: &str = "spherical_elevation";
const COLUMN_NAME_SPHERICAL_RANGE_STR: &str = "spherical_range";

#[derive(Debug, Clone, Copy, Eq, PartialEq, EnumIter)]
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
    TimestampSeconds,
    /// Nanoseconds since the last whole non-leap second
    TimestampNanoSeconds,
    /// Representation of the pulse return magnitude
    Intensity,
    /// Beam origin X coordinate of current laser shot
    BeamOriginX,
    /// Beam origin Y coordinate of current laser shot
    BeamOriginY,
    /// Beam origin Z coordinate of current laser shot
    BeamOriginZ,
    /// Red image channel value
    ColorRed,
    /// Green image channel value
    ColorGreen,
    /// Blue image channel value
    ColorBlue,
    /// Azimuth in context of spherical coordinates
    SphericalAzimuth,
    /// Elevation in context of spherical coordinates
    SphericalElevation,
    /// Range in context of spherical coordinates
    SphericalRange,
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
            COLUMN_NAME_TIMESTAMP_SEC_STR => Ok(PointDataColumnType::TimestampSeconds),
            COLUMN_NAME_TIMESTAMP_NANOSEC_STR => Ok(PointDataColumnType::TimestampNanoSeconds),
            COLUMN_NAME_INTENSITY_STR => Ok(PointDataColumnType::Intensity),
            COLUMN_NAME_BEAM_ORIGIN_X_STR => Ok(PointDataColumnType::BeamOriginX),
            COLUMN_NAME_BEAM_ORIGIN_Y_STR => Ok(PointDataColumnType::BeamOriginY),
            COLUMN_NAME_BEAM_ORIGIN_Z_STR => Ok(PointDataColumnType::BeamOriginZ),
            COLUMN_NAME_COLOR_RED_STR => Ok(PointDataColumnType::ColorRed),
            COLUMN_NAME_COLOR_GREEN_STR => Ok(PointDataColumnType::ColorGreen),
            COLUMN_NAME_COLOR_BLUE_STR => Ok(PointDataColumnType::ColorBlue),
            COLUMN_NAME_SPHERICAL_AZIMUTH_STR => Ok(PointDataColumnType::SphericalAzimuth),
            COLUMN_NAME_SPHERICAL_ELEVATION_STR => Ok(PointDataColumnType::SphericalElevation),
            COLUMN_NAME_SPHERICAL_RANGE_STR => Ok(PointDataColumnType::SphericalRange),
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
            PointDataColumnType::TimestampSeconds => COLUMN_NAME_TIMESTAMP_SEC_STR,
            PointDataColumnType::TimestampNanoSeconds => COLUMN_NAME_TIMESTAMP_NANOSEC_STR,
            PointDataColumnType::Intensity => COLUMN_NAME_INTENSITY_STR,
            PointDataColumnType::BeamOriginX => COLUMN_NAME_BEAM_ORIGIN_X_STR,
            PointDataColumnType::BeamOriginY => COLUMN_NAME_BEAM_ORIGIN_Y_STR,
            PointDataColumnType::BeamOriginZ => COLUMN_NAME_BEAM_ORIGIN_Z_STR,
            PointDataColumnType::ColorRed => COLUMN_NAME_COLOR_RED_STR,
            PointDataColumnType::ColorGreen => COLUMN_NAME_COLOR_GREEN_STR,
            PointDataColumnType::ColorBlue => COLUMN_NAME_COLOR_BLUE_STR,
            PointDataColumnType::SphericalAzimuth => COLUMN_NAME_SPHERICAL_AZIMUTH_STR,
            PointDataColumnType::SphericalElevation => COLUMN_NAME_SPHERICAL_ELEVATION_STR,
            PointDataColumnType::SphericalRange => COLUMN_NAME_SPHERICAL_RANGE_STR,
        }
    }

    pub fn data_frame_data_type(&self) -> DataType {
        match self {
            PointDataColumnType::X => DataType::Float64,
            PointDataColumnType::Y => DataType::Float64,
            PointDataColumnType::Z => DataType::Float64,
            PointDataColumnType::Id => DataType::UInt64,
            PointDataColumnType::FrameId => DataType::Categorical(None, Default::default()),
            PointDataColumnType::TimestampSeconds => DataType::Int64,
            PointDataColumnType::TimestampNanoSeconds => DataType::UInt32,
            PointDataColumnType::Intensity => DataType::Float32,
            PointDataColumnType::BeamOriginX => DataType::Float64,
            PointDataColumnType::BeamOriginY => DataType::Float64,
            PointDataColumnType::BeamOriginZ => DataType::Float64,
            PointDataColumnType::ColorRed => DataType::UInt16,
            PointDataColumnType::ColorGreen => DataType::UInt16,
            PointDataColumnType::ColorBlue => DataType::UInt16,
            PointDataColumnType::SphericalAzimuth => DataType::Float64,
            PointDataColumnType::SphericalElevation => DataType::Float64,
            PointDataColumnType::SphericalRange => DataType::Float64,
        }
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

        let column_names = data_frame.get_column_names();
        if column_names[0] != PointDataColumnType::X.as_str() {
            return Err(ColumnNameMisMatch(
                0,
                PointDataColumnType::X.as_str(),
                column_names[0].to_string(),
            ));
        }
        if column_names[1] != PointDataColumnType::Y.as_str() {
            return Err(ColumnNameMisMatch(
                1,
                PointDataColumnType::Y.as_str(),
                column_names[1].to_string(),
            ));
        }
        if column_names[2] != PointDataColumnType::Z.as_str() {
            return Err(ColumnNameMisMatch(
                2,
                PointDataColumnType::Z.as_str(),
                column_names[2].to_string(),
            ));
        }

        // check if column types are correct
        let data_frame_column_types: Vec<PointDataColumnType> = data_frame
            .get_column_names()
            .iter()
            .filter_map(|x| PointDataColumnType::from_str(x).ok())
            .collect();

        for current_column_type in data_frame_column_types {
            let current_series = data_frame
                .column(current_column_type.as_str())
                .expect("Column must exist");
            if current_series.dtype() != &current_column_type.data_frame_data_type() {
                return Err(TypeMisMatch(current_column_type.as_str()));
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

    pub fn get_frame_id_values(&self) -> Result<&CategoricalChunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::FrameId.as_str())?
            .categorical()
            .expect("type must be categorical");
        Ok(values)
    }

    pub fn get_timestamp_sec_values(&self) -> Result<&Int64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::TimestampSeconds.as_str())?
            .i64()
            .expect("type must be i64");
        Ok(values)
    }

    pub fn get_timestamp_nanosec_values(&self) -> Result<&UInt32Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::TimestampNanoSeconds.as_str())?
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

    pub fn get_beam_origin_x_values(&self) -> Result<&Float64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::BeamOriginX.as_str())?
            .f64()
            .expect("type must be f64");
        Ok(values)
    }

    pub fn get_beam_origin_y_values(&self) -> Result<&Float64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::BeamOriginY.as_str())?
            .f64()
            .expect("type must be f64");
        Ok(values)
    }

    pub fn get_beam_origin_z_values(&self) -> Result<&Float64Chunked, Error> {
        let values = self
            .data_frame
            .column(PointDataColumnType::BeamOriginZ.as_str())?
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
            .column(PointDataColumnType::TimestampSeconds.as_str())
            .is_ok()
    }

    pub fn contains_timestamp_nanosec_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::TimestampNanoSeconds.as_str())
            .is_ok()
    }

    pub fn contains_intensity_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::Intensity.as_str())
            .is_ok()
    }

    pub fn contains_beam_origin_x_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::BeamOriginX.as_str())
            .is_ok()
    }

    pub fn contains_beam_origin_y_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::BeamOriginY.as_str())
            .is_ok()
    }

    pub fn contains_beam_origin_z_column(&self) -> bool {
        self.data_frame
            .column(PointDataColumnType::BeamOriginZ.as_str())
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
}

impl PointData {
    pub fn contains_timestamps(&self) -> bool {
        self.contains_timestamp_sec_column() && self.contains_timestamp_nanosec_column()
    }

    pub fn contains_beam_origin(&self) -> bool {
        self.contains_beam_origin_x_column()
            && self.contains_beam_origin_y_column()
            && self.contains_beam_origin_z_column()
    }

    pub fn contains_colors(&self) -> bool {
        self.contains_color_red_column()
            && self.contains_color_green_column()
            && self.contains_color_blue_column()
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

    pub fn get_frame_ids(&self) -> Result<Vec<FrameId>, Error> {
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

        let times: Vec<DateTime<Utc>> = timestamp_sec_series
            .into_iter()
            .zip(timestamp_nanosec_series)
            .map(|t| Utc.timestamp_opt(t.0.unwrap(), t.1.unwrap()).unwrap())
            .collect();
        Ok(times)
    }

    /// Returns all points as a vector in the local coordinate frame.
    pub fn get_all_beam_origins(&self) -> Result<Vec<Point3<f64>>, Error> {
        let x_values = self.get_beam_origin_x_values()?;
        let y_values = self.get_beam_origin_y_values()?;
        let z_values = self.get_beam_origin_z_values()?;

        let all_beam_origins: Vec<Point3<f64>> = (0..self.data_frame.height())
            .into_par_iter()
            .map(|i: usize| {
                Point3::new(
                    x_values.get(i).unwrap(),
                    y_values.get(i).unwrap(),
                    z_values.get(i).unwrap(),
                )
            })
            .collect();

        Ok(all_beam_origins)
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
    pub fn get_distinct_frame_ids(&self) -> Result<HashSet<FrameId>, Error> {
        let values: HashSet<FrameId> = self
            .data_frame
            .column(PointDataColumnType::FrameId.as_str())?
            .unique()?
            .categorical()
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
        let points = self.get_all_points();
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
            return Err(ShapeMisMatch("should have the same height"));
        }

        let new_series = Series::new(PointDataColumnType::Id.as_str(), values);
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
            return Err(ShapeMisMatch(
                "values have a different length than point_data",
            ));
        }

        let new_series = Series::new(name, values);
        self.data_frame.with_column(new_series)?;
        Ok(())
    }

    /// Add a new column to this DataFrame or replace an existing one.
    pub fn add_u32_column(&mut self, name: &str, values: Vec<u32>) -> Result<(), Error> {
        if values.len() != self.data_frame.height() {
            return Err(ShapeMisMatch(
                "values have a different length than point_data",
            ));
        }

        let new_series = Series::new(name, values);
        self.data_frame.with_column(new_series)?;
        Ok(())
    }

    /// Add a new column to this DataFrame or replace an existing one.
    pub fn add_f32_column(&mut self, name: &str, values: Vec<f32>) -> Result<(), Error> {
        if values.len() != self.data_frame.height() {
            return Err(ShapeMisMatch(
                "values have a different length than point_data",
            ));
        }

        let new_series = Series::new(name, values);
        self.data_frame.with_column(new_series)?;
        Ok(())
    }

    /// Add a new column to this DataFrame or replace an existing one.
    pub fn add_f64_column(&mut self, name: &str, values: Vec<f64>) -> Result<(), Error> {
        if values.len() != self.data_frame.height() {
            return Err(ShapeMisMatch(
                "values have a different length than point_data",
            ));
        }

        let new_series = Series::new(name, values);
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
    pub fn extract_frame_ids(&self) -> Result<Vec<FrameId>, Error> {
        let frame_ids = self
            .get_frame_id_values()?
            .cast(&DataType::String)
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .map(|f| f.to_string().into())
            .collect();

        Ok(frame_ids)
    }

    pub fn extract_beam_origins(&self) -> Result<Vec<Point3<f64>>, Error> {
        let beam_x_values = self.get_beam_origin_x_values()?;
        let beam_y_values = self.get_beam_origin_y_values()?;
        let beam_z_values = self.get_beam_origin_z_values()?;

        let all_beam_origin: Vec<Point3<f64>> = (0..self.data_frame.height())
            .into_par_iter()
            .map(|i: usize| {
                Point3::new(
                    beam_x_values.get(i).unwrap(),
                    beam_y_values.get(i).unwrap(),
                    beam_z_values.get(i).unwrap(),
                )
            })
            .collect();

        Ok(all_beam_origin)
    }

    pub fn extract_timestamps(&self) -> Result<Vec<DateTime<Utc>>, Error> {
        let timestamp_seconds: &Int64Chunked = self.get_timestamp_sec_values()?;
        let timestamp_nanoseconds: &UInt32Chunked = self.get_timestamp_nanosec_values()?;

        let timestamps: Vec<DateTime<Utc>> = timestamp_seconds
            .into_iter()
            .zip(timestamp_nanoseconds)
            .map(|(current_seconds, current_nanoseconds)| {
                Utc.timestamp_opt(current_seconds.unwrap(), current_nanoseconds.unwrap())
                    .unwrap()
            })
            .collect();

        Ok(timestamps)
    }

    pub fn update_points_in_place(&mut self, points: Vec<Point3<f64>>) -> Result<(), Error> {
        if points.len() != self.data_frame.height() {
            return Err(ShapeMisMatch("points"));
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
            PointDataColumnType::X.as_str(),
            points.iter().map(|p| p.x).collect::<Vec<f64>>(),
        );
        let y_series = Series::new(
            PointDataColumnType::Y.as_str(),
            points.iter().map(|p| p.y).collect::<Vec<f64>>(),
        );
        let z_series = Series::new(
            PointDataColumnType::Z.as_str(),
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

    pub fn update_beam_origins_in_place(
        &mut self,
        beam_origins: Vec<Point3<f64>>,
    ) -> Result<(), Error> {
        if beam_origins.len() != self.data_frame.height() {
            return Err(ShapeMisMatch(
                "beam_origins has a different size than the point_data",
            ));
        }

        let beam_origin_x_series = Series::new(
            PointDataColumnType::X.as_str(),
            beam_origins.iter().map(|p| p.x).collect::<Vec<f64>>(),
        );
        let beam_origin_y_series = Series::new(
            PointDataColumnType::Y.as_str(),
            beam_origins.iter().map(|p| p.y).collect::<Vec<f64>>(),
        );
        let beam_origin_z_series = Series::new(
            PointDataColumnType::Z.as_str(),
            beam_origins.iter().map(|p| p.z).collect::<Vec<f64>>(),
        );
        self.data_frame.replace(
            PointDataColumnType::BeamOriginX.as_str(),
            beam_origin_x_series,
        )?;
        self.data_frame.replace(
            PointDataColumnType::BeamOriginY.as_str(),
            beam_origin_y_series,
        )?;
        self.data_frame.replace(
            PointDataColumnType::BeamOriginZ.as_str(),
            beam_origin_z_series,
        )?;

        Ok(())
    }

    pub fn add_spherical_points(
        &mut self,
        spherical_points: Vec<SphericalPoint3<f64>>,
    ) -> Result<(), Error> {
        if spherical_points.len() != self.data_frame.height() {
            return Err(ShapeMisMatch(
                "spherical_points has a different size than the point_data",
            ));
        }

        let spherical_azimuth_series = Series::new(
            PointDataColumnType::SphericalAzimuth.as_str(),
            spherical_points.iter().map(|p| p.phi).collect::<Vec<f64>>(),
        );
        let spherical_elevation_series = Series::new(
            PointDataColumnType::SphericalElevation.as_str(),
            spherical_points
                .iter()
                .map(|p| p.theta)
                .collect::<Vec<f64>>(),
        );
        let spherical_range_series = Series::new(
            PointDataColumnType::SphericalRange.as_str(),
            spherical_points.iter().map(|p| p.r).collect::<Vec<f64>>(),
        );
        self.data_frame.with_column(spherical_azimuth_series)?;
        self.data_frame.with_column(spherical_elevation_series)?;
        self.data_frame.with_column(spherical_range_series)?;

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
            return Err(ShapeMisMatch(
                "frame_ids has a different size than the point_data",
            ));
        }

        let frame_id_series = Series::new(
            PointDataColumnType::FrameId.as_str(),
            frame_ids
                .into_iter()
                .map(|x| x.to_string())
                .collect::<Vec<String>>(),
        )
        .cast(&DataType::Categorical(None, Default::default()))
        .unwrap();
        self.data_frame.with_column(frame_id_series)?;

        Ok(())
    }

    pub fn add_unique_color(&mut self, color: palette::Srgb<u16>) -> Result<(), Error> {
        let colors = vec![color; self.data_frame.height()];
        self.add_colors(colors)?;

        Ok(())
    }

    pub fn add_colors(&mut self, colors: Vec<palette::Srgb<u16>>) -> Result<(), Error> {
        if colors.len() != self.data_frame.height() {
            return Err(ShapeMisMatch(
                "colors has a different size than the point_data",
            ));
        }

        let color_red_series = Series::new(
            PointDataColumnType::ColorRed.as_str(),
            colors.iter().map(|p| p.red).collect::<Vec<u16>>(),
        );
        let color_green_series = Series::new(
            PointDataColumnType::ColorGreen.as_str(),
            colors.iter().map(|p| p.green).collect::<Vec<u16>>(),
        );
        let color_blue_series = Series::new(
            PointDataColumnType::ColorBlue.as_str(),
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
            return Err(Error::LowerBoundExceedsUpperBound);
        }
        if beam_length_min == beam_length_max {
            return Err(Error::LowerBoundEqualsUpperBound);
        }
        if !self.contains_beam_origin() {
            return Err(Error::NoBeamOriginColumn);
        }

        let filtered_data_frame = self
            .data_frame
            .clone()
            .lazy()
            .filter(
                col(PointDataColumnType::X.as_str())
                    .sub(col(PointDataColumnType::BeamOriginX.as_str()))
                    .pow(2)
                    .add(
                        col(PointDataColumnType::Y.as_str())
                            .sub(col(PointDataColumnType::BeamOriginY.as_str()))
                            .pow(2),
                    )
                    .add(
                        col(PointDataColumnType::Z.as_str())
                            .sub(col(PointDataColumnType::BeamOriginZ.as_str()))
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

        // println!("{:?}", frame_id);
        //println!("{:?}.{:?}", timestamp_seconds, timestamp_nanoseconds);
        //let temp_transform_id = TransformId::new(FrameId::from("slam_map"), FrameId::from("base_link"));
        //let temp_isometry = graph.get_isometry(&temp_transform_id);
        //println!("{:?}", temp_isometry);
        //println!("");

        let transformed_points: Vec<Point3<f64>> = self
            .get_all_points()
            .par_iter()
            .map(|p| isometry * p)
            .collect();
        self.update_points_in_place(transformed_points)?;

        if let Ok(all_beam_origins) = &self.extract_beam_origins() {
            let transformed_beam_origins: Vec<Point3<f64>> =
                all_beam_origins.par_iter().map(|p| isometry * p).collect();
            self.update_beam_origins_in_place(transformed_beam_origins)?;
        }

        Ok(())
    }
}
