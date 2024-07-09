use crate::Error::{NoData, ShapeMisMatch};
use crate::{Error, PointDataColumnType};
use chrono::{DateTime, Timelike, Utc};
use nalgebra::Point3;
use palette::Srgb;
use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::Series;

pub struct PointDataColumns {
    pub point: Vec<Point3<f64>>,
    pub id: Option<Vec<u64>>,
    pub frame_id: Option<Vec<String>>,
    pub timestamp: Option<Vec<DateTime<Utc>>>,
    pub intensity: Option<Vec<f32>>,
    pub beam_origin: Option<Vec<Point3<f64>>>,
    pub color: Option<Vec<Srgb<u16>>>,
}

impl PointDataColumns {
    pub fn new(
        point: Vec<Point3<f64>>,
        id: Option<Vec<u64>>,
        frame_id: Option<Vec<String>>,
        timestamp: Option<Vec<DateTime<Utc>>>,
        intensity: Option<Vec<f32>>,
        beam_origin: Option<Vec<Point3<f64>>>,
        color: Option<Vec<Srgb<u16>>>,
    ) -> Result<Self, Error> {
        if point.is_empty() {
            return Err(NoData("point"));
        }
        let total_length = point.len();

        if let Some(id) = &id {
            if id.len() != total_length {
                return Err(ShapeMisMatch(
                    "id vector has a different length than the point vector",
                ));
            }
        }
        if let Some(frame_id_entries) = &frame_id {
            if frame_id_entries.len() != total_length {
                return Err(ShapeMisMatch(
                    "frame_id vector has a different length than the point vector",
                ));
            }
        }
        if let Some(timestamp_entries) = &timestamp {
            if timestamp_entries.len() != total_length {
                return Err(ShapeMisMatch(
                    "frame_id vector has a different length than the point vector",
                ));
            }
        }
        if let Some(intensity_entries) = &intensity {
            if intensity_entries.len() != total_length {
                return Err(ShapeMisMatch(
                    "intensity vector has a different length than the point vector",
                ));
            }
        }
        if let Some(beam_origin_entries) = &beam_origin {
            if beam_origin_entries.len() != total_length {
                return Err(ShapeMisMatch(
                    "beam_origin vector has a different length than the point vector",
                ));
            }
        }
        if let Some(color_entries) = &color {
            if color_entries.len() != total_length {
                return Err(ShapeMisMatch(
                    "color vector has a different length than the point vector",
                ));
            }
        }

        Ok(Self {
            point,
            id,
            frame_id,
            timestamp,
            intensity,
            beam_origin,
            color,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.point.is_empty()
    }

    pub fn len(&self) -> usize {
        self.point.len()
    }

    pub fn get_as_data_frame(&self) -> DataFrame {
        let mut columns = self.get_xyz_series();

        if let Some(id) = &self.id {
            let id_series = Series::new(PointDataColumnType::Id.as_str(), id);
            columns.push(id_series);
        }

        if let Some(frame_id) = &self.frame_id {
            let frame_id_series = Series::new(PointDataColumnType::FrameId.as_str(), frame_id);
            let frame_id_series = frame_id_series
                .cast(&DataType::Categorical(None, Default::default()))
                .unwrap();
            columns.push(frame_id_series);
        }

        if let Some(timestamp_entries) = &self.timestamp {
            let timestamp_seconds_series = Series::new(
                PointDataColumnType::TimestampSeconds.as_str(),
                timestamp_entries
                    .iter()
                    .map(|t| t.timestamp())
                    .collect::<Vec<i64>>(),
            );
            columns.push(timestamp_seconds_series);

            let timestamp_nanoseconds_series = Series::new(
                PointDataColumnType::TimestampNanoSeconds.as_str(),
                timestamp_entries
                    .iter()
                    .map(|t| t.nanosecond())
                    .collect::<Vec<u32>>(),
            );
            columns.push(timestamp_nanoseconds_series);
        }

        if let Some(intensity) = &self.intensity {
            let intensity_series = Series::new(PointDataColumnType::Intensity.as_str(), intensity);
            columns.push(intensity_series);
        }

        if self.beam_origin.is_some() {
            columns.append(&mut self.get_beam_origin_xyz_series().unwrap());
        }

        if self.color.is_some() {
            columns.append(&mut self.get_color_series().unwrap());
        }

        DataFrame::new(columns).unwrap()
    }

    pub fn get_xyz_series(&self) -> Vec<Series> {
        vec![
            Series::new(
                PointDataColumnType::X.as_str(),
                self.point.iter().map(|p| p.x).collect::<Vec<f64>>(),
            ),
            Series::new(
                PointDataColumnType::Y.as_str(),
                self.point.iter().map(|p| p.y).collect::<Vec<f64>>(),
            ),
            Series::new(
                PointDataColumnType::Z.as_str(),
                self.point.iter().map(|p| p.z).collect::<Vec<f64>>(),
            ),
        ]
    }

    pub fn get_beam_origin_xyz_series(&self) -> Option<Vec<Series>> {
        let beam_origin = self.beam_origin.as_ref()?;

        Some(vec![
            Series::new(
                PointDataColumnType::BeamOriginX.as_str(),
                beam_origin.iter().map(|p| p.x).collect::<Vec<f64>>(),
            ),
            Series::new(
                PointDataColumnType::BeamOriginY.as_str(),
                beam_origin.iter().map(|p| p.y).collect::<Vec<f64>>(),
            ),
            Series::new(
                PointDataColumnType::BeamOriginZ.as_str(),
                beam_origin.iter().map(|p| p.z).collect::<Vec<f64>>(),
            ),
        ])
    }

    pub fn get_color_series(&self) -> Option<Vec<Series>> {
        let color = self.color.as_ref()?;

        Some(vec![
            Series::new(
                PointDataColumnType::ColorRed.as_str(),
                color.iter().map(|c| c.red).collect::<Vec<u16>>(),
            ),
            Series::new(
                PointDataColumnType::ColorGreen.as_str(),
                color.iter().map(|c| c.green).collect::<Vec<u16>>(),
            ),
            Series::new(
                PointDataColumnType::ColorBlue.as_str(),
                color.iter().map(|c| c.blue).collect::<Vec<u16>>(),
            ),
        ])
    }
}
