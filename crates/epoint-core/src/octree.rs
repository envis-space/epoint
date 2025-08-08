use crate::{Error, PointData};
use ecoord::HasAabb;
use ecoord::octree::{OctantIndex, Octree};
use itertools::Itertools;
use nalgebra::Point3;

#[derive(Debug, Clone, Copy, PartialEq)]
struct PointWithIndex {
    index: usize,
    point: Point3<f64>,
}

impl HasAabb for PointWithIndex {
    fn center(&self) -> Point3<f64> {
        self.point
    }

    fn min(&self) -> Point3<f64> {
        self.point
    }

    fn max(&self) -> Point3<f64> {
        self.point
    }
}

impl PointData {
    pub fn compute_octree(
        &mut self,
        max_items_per_octant: usize,
        shuffle_seed_number: Option<u64>,
    ) -> Result<(), Error> {
        let all_points: Vec<PointWithIndex> = self
            .get_all_points()
            .into_iter()
            .enumerate()
            .map(|(index, point)| PointWithIndex { index, point })
            .collect();

        let octree = Octree::new(all_points, max_items_per_octant, shuffle_seed_number)?;

        let octant_indices: Vec<(usize, OctantIndex)> = octree
            .cells()
            .iter()
            .flat_map(|(octant_index, points)| points.iter().map(move |p| (p.index, *octant_index)))
            .sorted_by_key(|(i, _)| *i)
            .collect();
        self.add_octant_indices(octant_indices.into_iter().map(|p| p.1).collect())?;

        Ok(())
    }
}
