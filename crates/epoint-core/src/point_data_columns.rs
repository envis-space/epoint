use crate::Error::{NoData, ShapeMisMatch};
use crate::{Error, PointDataColumnType};
use chrono::{DateTime, Timelike, Utc};
use nalgebra::Point3;
use palette::Srgb;
use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::prelude::{Column, NamedFrom};

pub struct PointDataColumns {
    pub point: Vec<Point3<f64>>,
    pub id: Option<Vec<u64>>,
    pub frame_id: Option<Vec<String>>,
    pub timestamp: Option<Vec<DateTime<Utc>>>,
    pub intensity: Option<Vec<f32>>,
    pub sensor_translation: Option<Vec<Point3<f64>>>,
    pub color: Option<Vec<Srgb<u16>>>,
}

impl PointDataColumns {
    pub fn new(
        point: Vec<Point3<f64>>,
        id: Option<Vec<u64>>,
        frame_id: Option<Vec<String>>,
        timestamp: Option<Vec<DateTime<Utc>>>,
        intensity: Option<Vec<f32>>,
        sensor_translation: Option<Vec<Point3<f64>>>,
        color: Option<Vec<Srgb<u16>>>,
    ) -> Result<Self, Error> {
        if point.is_empty() {
            return Err(NoData("point"));
        }
        let total_length = point.len();

        if let Some(id) = &id
            && id.len() != total_length
        {
            return Err(ShapeMisMatch(
                "id vector has a different length than the point vector",
            ));
        }
        if let Some(frame_id_entries) = &frame_id
            && frame_id_entries.len() != total_length
        {
            return Err(ShapeMisMatch(
                "frame_id vector has a different length than the point vector",
            ));
        }
        if let Some(timestamp_entries) = &timestamp
            && timestamp_entries.len() != total_length
        {
            return Err(ShapeMisMatch(
                "frame_id vector has a different length than the point vector",
            ));
        }
        if let Some(intensity_entries) = &intensity
            && intensity_entries.len() != total_length
        {
            return Err(ShapeMisMatch(
                "intensity vector has a different length than the point vector",
            ));
        }
        if let Some(sensor_translation_entries) = &sensor_translation
            && sensor_translation_entries.len() != total_length
        {
            return Err(ShapeMisMatch(
                "sensor_translation vector has a different length than the point vector",
            ));
        }
        if let Some(color_entries) = &color
            && color_entries.len() != total_length
        {
            return Err(ShapeMisMatch(
                "color vector has a different length than the point vector",
            ));
        }

        Ok(Self {
            point,
            id,
            frame_id,
            timestamp,
            intensity,
            sensor_translation,
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
        let mut columns = self.get_xyz_columns();

        if let Some(id) = &self.id {
            let id_column = Column::new(PointDataColumnType::Id.into(), id);
            columns.push(id_column);
        }

        if let Some(frame_id) = &self.frame_id {
            let frame_id_column = Column::new(PointDataColumnType::FrameId.into(), frame_id);
            let frame_id_column = frame_id_column
                .cast(&DataType::Categorical(None, Default::default()))
                .unwrap();
            columns.push(frame_id_column);
        }

        if let Some(timestamp_entries) = &self.timestamp {
            let timestamp_seconds_column = Column::new(
                PointDataColumnType::TimestampSeconds.into(),
                timestamp_entries
                    .iter()
                    .map(|t| t.timestamp())
                    .collect::<Vec<i64>>(),
            );
            columns.push(timestamp_seconds_column);

            let timestamp_nanoseconds_column = Column::new(
                PointDataColumnType::TimestampNanoSeconds.into(),
                timestamp_entries
                    .iter()
                    .map(|t| t.nanosecond())
                    .collect::<Vec<u32>>(),
            );
            columns.push(timestamp_nanoseconds_column);
        }

        if let Some(intensity) = &self.intensity {
            let intensity_column = Column::new(PointDataColumnType::Intensity.into(), intensity);
            columns.push(intensity_column);
        }

        if self.sensor_translation.is_some() {
            columns.append(&mut self.get_sensor_translation_xyz_columns().unwrap());
        }

        if self.color.is_some() {
            columns.append(&mut self.get_color_columns().unwrap());
        }

        DataFrame::new(columns).unwrap()
    }

    pub fn get_xyz_columns(&self) -> Vec<Column> {
        vec![
            Column::new(
                PointDataColumnType::X.into(),
                self.point.iter().map(|p| p.x).collect::<Vec<f64>>(),
            ),
            Column::new(
                PointDataColumnType::Y.into(),
                self.point.iter().map(|p| p.y).collect::<Vec<f64>>(),
            ),
            Column::new(
                PointDataColumnType::Z.into(),
                self.point.iter().map(|p| p.z).collect::<Vec<f64>>(),
            ),
        ]
    }

    pub fn get_sensor_translation_xyz_columns(&self) -> Option<Vec<Column>> {
        let sensor_translation = self.sensor_translation.as_ref()?;

        Some(vec![
            Column::new(
                PointDataColumnType::SensorTranslationX.into(),
                sensor_translation.iter().map(|p| p.x).collect::<Vec<f64>>(),
            ),
            Column::new(
                PointDataColumnType::SensorTranslationY.into(),
                sensor_translation.iter().map(|p| p.y).collect::<Vec<f64>>(),
            ),
            Column::new(
                PointDataColumnType::SensorTranslationZ.into(),
                sensor_translation.iter().map(|p| p.z).collect::<Vec<f64>>(),
            ),
        ])
    }

    pub fn get_color_columns(&self) -> Option<Vec<Column>> {
        let color = self.color.as_ref()?;

        Some(vec![
            Column::new(
                PointDataColumnType::ColorRed.into(),
                color.iter().map(|c| c.red).collect::<Vec<u16>>(),
            ),
            Column::new(
                PointDataColumnType::ColorGreen.into(),
                color.iter().map(|c| c.green).collect::<Vec<u16>>(),
            ),
            Column::new(
                PointDataColumnType::ColorBlue.into(),
                color.iter().map(|c| c.blue).collect::<Vec<u16>>(),
            ),
        ])
    }
}
