use crate::Error;
use crate::Error::InvalidNumber;
use crate::point_data::PointData;
use polars::datatypes::BooleanChunked;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rayon::prelude::*;
use std::collections::HashSet;

pub fn deterministic_divide(
    point_data: &PointData,
    target_size: usize,
    seed_number: Option<u64>,
) -> Result<(PointData, PointData), Error> {
    let rng = ChaCha8Rng::seed_from_u64(seed_number.unwrap_or_default());
    let row_indices = generate_random_numbers(rng, point_data.height(), target_size)?;

    let target_point_data_mask: BooleanChunked = (0..point_data.data_frame.height())
        .into_par_iter()
        .map(|x| row_indices.contains(&x))
        .collect();
    let target_point_data = point_data.filter_by_boolean_mask(&target_point_data_mask)?;

    let remaining_point_data_mask: BooleanChunked = target_point_data_mask
        .into_iter()
        .map(|x| !x.unwrap())
        .collect();
    let remaining_point_data = point_data.filter_by_boolean_mask(&remaining_point_data_mask)?;

    Ok((target_point_data, remaining_point_data))
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
