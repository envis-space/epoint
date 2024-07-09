use crate::bounding_box::AxisAlignedBoundingCube;
use crate::point_data::PointData;
use crate::utility::deterministic_divide;
use crate::{Error, PointCloud};

use std::collections::HashMap;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

// https://en.wikipedia.org/wiki/Octant_(solid_geometry)
#[derive(Eq, PartialEq, Debug, Copy, Clone, EnumIter, Hash)]
pub enum OctantIdentifier {
    // Positive x, positive y, positive z
    Zero,
    // Negative x, positive y, positive z
    One,
    // Negative x, negative y, positive z
    Two,
    // Positive x, negative y, positive z
    Three,
    // Positive x, positive y, negative z
    Four,
    // Positive x, negative y, negative z
    Five,
    // Negative x, negative y, negative z
    Six,
    // Positive x, positive y, negative z
    Seven,
}

impl OctantIdentifier {
    pub fn as_str(&self) -> &'static str {
        match self {
            OctantIdentifier::Zero => "0",
            OctantIdentifier::One => "1",
            OctantIdentifier::Two => "2",
            OctantIdentifier::Three => "3",
            OctantIdentifier::Four => "4",
            OctantIdentifier::Five => "5",
            OctantIdentifier::Six => "6",
            OctantIdentifier::Seven => "7",
        }
    }

    pub fn axis_x_sign(&self) -> bool {
        match self {
            OctantIdentifier::Zero => true,
            OctantIdentifier::One => false,
            OctantIdentifier::Two => false,
            OctantIdentifier::Three => true,
            OctantIdentifier::Four => true,
            OctantIdentifier::Five => false,
            OctantIdentifier::Six => false,
            OctantIdentifier::Seven => true,
        }
    }

    pub fn axis_y_sign(&self) -> bool {
        match self {
            OctantIdentifier::Zero => true,
            OctantIdentifier::One => true,
            OctantIdentifier::Two => false,
            OctantIdentifier::Three => false,
            OctantIdentifier::Four => false,
            OctantIdentifier::Five => false,
            OctantIdentifier::Six => true,
            OctantIdentifier::Seven => true,
        }
    }

    pub fn axis_z_sign(&self) -> bool {
        match self {
            OctantIdentifier::Zero => true,
            OctantIdentifier::One => true,
            OctantIdentifier::Two => true,
            OctantIdentifier::Three => true,
            OctantIdentifier::Four => false,
            OctantIdentifier::Five => false,
            OctantIdentifier::Six => false,
            OctantIdentifier::Seven => false,
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct CellIdentifier {
    octants: Vec<OctantIdentifier>,
}

impl CellIdentifier {
    pub fn origin() -> Self {
        Self {
            octants: Vec::new(),
        }
    }

    pub fn octants(&self) -> &Vec<OctantIdentifier> {
        &self.octants
    }

    pub fn to_string(&self) -> String {
        self.octants.iter().map(|x| x.as_str()).collect::<String>()
    }

    pub fn append(&self, id: OctantIdentifier) -> Self {
        let mut new = self.octants.clone();
        new.push(id);

        Self { octants: new }
    }

    pub fn get_children(&self) -> [CellIdentifier; 8] {
        [
            self.append(OctantIdentifier::Zero),
            self.append(OctantIdentifier::One),
            self.append(OctantIdentifier::Two),
            self.append(OctantIdentifier::Three),
            self.append(OctantIdentifier::Four),
            self.append(OctantIdentifier::Five),
            self.append(OctantIdentifier::Six),
            self.append(OctantIdentifier::Seven),
        ]
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PointCloudOctree {
    cells: HashMap<CellIdentifier, (AxisAlignedBoundingCube, PointData)>,
}

impl PointCloudOctree {
    pub fn from_point_cloud(
        point_cloud: PointCloud,
        max_point_cloud_size: usize,
        seed_number: Option<u64>,
    ) -> Result<Self, Error> {
        let bounding_cube = AxisAlignedBoundingCube::from_bounding_box(
            &point_cloud.point_data.get_axis_aligned_bounding_box(),
        );

        let cells = derive_octants(
            CellIdentifier::origin(),
            point_cloud.point_data,
            bounding_cube,
            max_point_cloud_size,
            seed_number,
        )?;
        //let _a = arena.new_node(point_cloud.point_data);

        Ok(Self { cells })
    }

    pub fn cells(&self) -> &HashMap<CellIdentifier, (AxisAlignedBoundingCube, PointData)> {
        &self.cells
    }

    pub fn get_cell(
        &self,
        cell_identifier: &CellIdentifier,
    ) -> Option<&(AxisAlignedBoundingCube, PointData)> {
        self.cells.get(cell_identifier)
    }

    pub fn number_of_cells(&self) -> usize {
        self.cells.len()
    }
}

fn derive_octants(
    id: CellIdentifier,
    point_data: PointData,
    bounding_cube: AxisAlignedBoundingCube,
    max_point_cloud_size: usize,
    seed_number: Option<u64>,
) -> Result<HashMap<CellIdentifier, (AxisAlignedBoundingCube, PointData)>, Error> {
    //if point_cloud.size() <= max_point_cloud_size {}
    let mut cell_collection: HashMap<CellIdentifier, (AxisAlignedBoundingCube, PointData)> =
        HashMap::new();

    if point_data.height() <= max_point_cloud_size {
        cell_collection.insert(id, (bounding_cube, point_data));
        return Ok(cell_collection);
    }

    let (base_point_data, remaining_point_data) =
        deterministic_divide(&point_data, max_point_cloud_size, seed_number)?;
    cell_collection.insert(id.clone(), (bounding_cube, base_point_data));

    // let _a = arena.new_node(base_point_data);

    for current_octant_id in OctantIdentifier::iter() {
        let current_octant_cube = bounding_cube.get_sub_cube(
            current_octant_id.axis_x_sign(),
            current_octant_id.axis_y_sign(),
            current_octant_id.axis_z_sign(),
        );
        let current_octant_point_data = remaining_point_data.filter_by_bounds(
            current_octant_cube.get_lower_bound(),
            current_octant_cube.get_upper_bound(),
        )?;
        if let Some(current_octant_point_data) = current_octant_point_data {
            let current_octant_cell_id = id.clone().append(current_octant_id);
            let n = derive_octants(
                current_octant_cell_id,
                current_octant_point_data,
                current_octant_cube,
                max_point_cloud_size,
                seed_number,
            )?;
            cell_collection.extend(n);

            // arena.new_node(current_octant_point_data);
        }
    }

    Ok(cell_collection)
}
