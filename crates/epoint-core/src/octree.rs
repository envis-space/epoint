use crate::{Error, PointCloud, PointData};
use ecoord::HasAabb;
use ecoord::octree::{OctantIndex, Octree};
use itertools::Itertools;
use nalgebra::Point3;
use polars::prelude::NewChunkedArray;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PointWithIndex {
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

#[derive(Debug, Clone)]
pub struct PointCloudOctree {
    pub point_cloud: PointCloud,
    pub octree: Octree<PointWithIndex>,
}

impl PointCloudOctree {
    pub fn new(
        point_cloud: PointCloud,
        max_points_per_octant: usize,
        shuffle_seed_number: Option<u64>,
    ) -> Result<Self, Error> {
        let all_points: Vec<PointWithIndex> = point_cloud
            .point_data
            .get_all_points()
            .into_iter()
            .enumerate()
            .map(|(index, point)| PointWithIndex { index, point })
            .collect();
        let octree = Octree::new(all_points, max_points_per_octant, shuffle_seed_number)?;

        let point_cloud_octree = PointCloudOctree {
            point_cloud,
            octree,
        };
        Ok(point_cloud_octree)
    }

    /// Returns the set of octant indices that contain data.
    pub fn cell_indices(&self) -> HashSet<OctantIndex> {
        self.octree.cell_indices()
    }

    pub fn extract_octant(&self, index: OctantIndex) -> Result<PointCloud, Error> {
        let cell_content = self
            .octree
            .cell(index)
            .ok_or(Error::NoData("selected octant"))?;
        let indices = cell_content.iter().map(|p| p.index as u32);
        let idx_series = polars::prelude::UInt32Chunked::from_iter_values("idx".into(), indices);
        let filtered_data_frame = self.point_cloud.point_data.data_frame.take(&idx_series)?;

        PointCloud::from_data_frame(
            filtered_data_frame,
            self.point_cloud.info.clone(),
            self.point_cloud.reference_frames.clone(),
        )
    }
}
