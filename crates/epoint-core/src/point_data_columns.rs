use crate::Error;
use crate::Error::{NoData, ShapeMisMatch};
use chrono::{DateTime, Timelike, Utc};
use nalgebra::Point3;
use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::Series;

pub struct PointDataColumns {
    pub points: Vec<Point3<f64>>,
    pub frame_id: Option<Vec<String>>,
    pub timestamp: Option<Vec<DateTime<Utc>>>,
    pub intensity: Option<Vec<f32>>,
}

impl PointDataColumns {
    pub fn new(
        points: Vec<Point3<f64>>,
        frame_id: Option<Vec<String>>,
        timestamp: Option<Vec<DateTime<Utc>>>,
        intensity: Option<Vec<f32>>,
    ) -> Result<Self, Error> {
        if points.is_empty() {
            return Err(NoData("point_data"));
        }

        if let Some(frame_id_entries) = &frame_id {
            if frame_id_entries.len() != points.len() {
                return Err(ShapeMisMatch);
            }
        }

        if let Some(timestamp_entries) = &timestamp {
            if timestamp_entries.len() != points.len() {
                return Err(ShapeMisMatch);
            }
        }

        if let Some(intensity_entries) = &intensity {
            if intensity_entries.len() != points.len() {
                return Err(ShapeMisMatch);
            }
        }

        Ok(Self {
            points,
            frame_id,
            timestamp,
            intensity,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    pub fn len(&self) -> usize {
        self.points.len()
    }

    pub fn get_as_data_frame(&self) -> DataFrame {
        let mut columns = vec![
            self.get_x_series(),
            self.get_y_series(),
            self.get_z_series(),
        ];

        if let Some(frame_id) = &self.frame_id {
            let frame_id_series = Series::new(PointDataColumnNames::FrameId.as_str(), frame_id);
            let frame_id_series = frame_id_series.cast(&DataType::Categorical(None)).unwrap();
            columns.push(frame_id_series);
        }

        if let Some(timestamp_entries) = &self.timestamp {
            let timestamp_seconds_series = Series::new(
                PointDataColumnNames::TimestampSeconds.as_str(),
                timestamp_entries
                    .iter()
                    .map(|t| t.timestamp())
                    .collect::<Vec<i64>>(),
            );
            columns.push(timestamp_seconds_series);

            let timestamp_nanoseconds_series = Series::new(
                PointDataColumnNames::TimestampNanoSeconds.as_str(),
                timestamp_entries
                    .iter()
                    .map(|t| t.nanosecond())
                    .collect::<Vec<u32>>(),
            );
            columns.push(timestamp_nanoseconds_series);
        }

        if let Some(intensity) = &self.intensity {
            let intensity_series = Series::new(PointDataColumnNames::Intensity.as_str(), intensity);
            columns.push(intensity_series);
        }

        DataFrame::new(columns).unwrap()
    }

    pub fn get_x_series(&self) -> Series {
        Series::new(
            PointDataColumnNames::X.as_str(),
            self.points.iter().map(|p| p.x).collect::<Vec<f64>>(),
        )
    }

    pub fn get_y_series(&self) -> Series {
        Series::new(
            PointDataColumnNames::Y.as_str(),
            self.points.iter().map(|p| p.y).collect::<Vec<f64>>(),
        )
    }

    pub fn get_z_series(&self) -> Series {
        Series::new(
            PointDataColumnNames::Z.as_str(),
            self.points.iter().map(|p| p.z).collect::<Vec<f64>>(),
        )
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PointDataColumnNames {
    X,
    Y,
    Z,
    FrameId,
    TimestampSeconds,
    TimestampNanoSeconds,
    Intensity,
}

impl PointDataColumnNames {
    pub fn as_str(&self) -> &'static str {
        match self {
            PointDataColumnNames::X => "x",
            PointDataColumnNames::Y => "y",
            PointDataColumnNames::Z => "z",
            PointDataColumnNames::FrameId => "frame_id",
            PointDataColumnNames::TimestampSeconds => "timestamp_sec",
            PointDataColumnNames::TimestampNanoSeconds => "timestamp_nanosec",
            PointDataColumnNames::Intensity => "intensity",
        }
    }
}
