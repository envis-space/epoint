use epoint_core::PointCloud;
use std::collections::HashSet;

use crate::Error;
use crate::Error::InvalidNumber;
use epoint_core::PointDataColumnType;
use nalgebra::{Isometry3, Point3, Vector3};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rayon::prelude::*;

pub fn translate(point_cloud: &PointCloud, translation: Vector3<f64>) -> Result<PointCloud, Error> {
    let mut translated_data = point_cloud.point_data.data_frame.clone();
    translated_data.apply(PointDataColumnType::X.as_str(), |x| x + translation.x)?;
    translated_data.apply(PointDataColumnType::Y.as_str(), |y| y + translation.y)?;
    translated_data.apply(PointDataColumnType::Z.as_str(), |z| z + translation.z)?;

    if point_cloud.contains_sensor_translation() {
        translated_data.apply(PointDataColumnType::SensorTranslationX.as_str(), |x| {
            x + translation.x
        })?;
        translated_data.apply(PointDataColumnType::SensorTranslationY.as_str(), |y| {
            y + translation.y
        })?;
        translated_data.apply(PointDataColumnType::SensorTranslationZ.as_str(), |z| {
            z + translation.z
        })?;
    }

    let info = point_cloud.info().clone();
    let frames = point_cloud.transform_tree().clone();
    let point_cloud = PointCloud::from_data_frame(translated_data, info, frames)?;
    Ok(point_cloud)
}

pub fn apply_isometry(
    point_cloud: &PointCloud,
    isometry: Isometry3<f64>,
) -> Result<PointCloud, Error> {
    let transformed_points: Vec<Point3<f64>> = point_cloud
        .point_data
        .get_all_points()
        .par_iter()
        .map(|p| isometry * p)
        .collect();
    let mut transformed_point_cloud = point_cloud.clone();
    transformed_point_cloud
        .point_data
        .update_points_in_place(transformed_points)?;

    if let Ok(all_sensor_translations) = point_cloud.point_data.get_all_sensor_translations() {
        let transformed_sensor_translations: Vec<Point3<f64>> = all_sensor_translations
            .par_iter()
            .map(|p| isometry * p)
            .collect();

        transformed_point_cloud
            .point_data
            .update_sensor_translations_in_place(transformed_sensor_translations)?;
    }

    Ok(transformed_point_cloud)
}

pub fn deterministic_downsample(
    point_cloud: &PointCloud,
    target_size: usize,
    seed_number: Option<u64>,
) -> Result<PointCloud, Error> {
    if point_cloud.size() < target_size {
        return Ok(point_cloud.clone());
    }

    let rng = ChaCha8Rng::seed_from_u64(seed_number.unwrap_or_default());
    let row_indices = generate_random_numbers(rng, point_cloud.size(), target_size)?;

    let downsampled_point_cloud = point_cloud.filter_by_row_indices(row_indices)?;
    Ok(downsampled_point_cloud)
}

fn generate_random_numbers(
    mut rng: ChaCha8Rng,
    number_max: usize,
    len: usize,
) -> Result<HashSet<usize>, Error> {
    if number_max < len {
        return Err(InvalidNumber);
    }

    let mut numbers: HashSet<usize> = HashSet::with_capacity(len);
    while numbers.len() < len {
        let n: usize = rng.random_range(0..number_max);
        numbers.insert(n);
    }
    Ok(numbers)
}
