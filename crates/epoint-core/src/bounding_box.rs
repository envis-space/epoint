use crate::Error;
use crate::Error::{InvalidNumber, LowerBoundEqualsUpperBound, LowerBoundExceedsUpperBound};
use nalgebra::{Point3, Vector3};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AxisAlignedBoundingBox {
    lower_bound: Point3<f64>,
    upper_bound: Point3<f64>,
}

impl AxisAlignedBoundingBox {
    pub fn new(lower_bound: Point3<f64>, upper_bound: Point3<f64>) -> Result<Self, Error> {
        if lower_bound > upper_bound {
            return Err(LowerBoundExceedsUpperBound);
        }
        if lower_bound == upper_bound {
            return Err(LowerBoundEqualsUpperBound);
        }

        Ok(Self {
            lower_bound,
            upper_bound,
        })
    }

    pub fn lower_bound(&self) -> Point3<f64> {
        self.lower_bound
    }

    pub fn upper_bound(&self) -> Point3<f64> {
        self.upper_bound
    }

    pub fn diagonal(&self) -> Vector3<f64> {
        self.upper_bound - self.lower_bound
    }

    pub fn get_center(&self) -> Point3<f64> {
        let diagonal = self.upper_bound - self.lower_bound;
        self.lower_bound + diagonal / 2.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AxisAlignedBoundingCube {
    center: Point3<f64>,
    edge_length: f64,
}

impl AxisAlignedBoundingCube {
    pub fn new(center: Point3<f64>, edge_length: f64) -> Result<Self, Error> {
        if edge_length <= 0.0 {
            return Err(InvalidNumber);
        }

        Ok(Self {
            center,
            edge_length,
        })
    }

    pub fn from_bounding_box(bounding_box: &AxisAlignedBoundingBox) -> Self {
        let center = bounding_box.get_center();
        let diagonal = bounding_box.diagonal();
        let edge_length = diagonal.x.max(diagonal.y).max(diagonal.z);

        Self {
            center,
            edge_length,
        }
    }

    pub fn center(&self) -> Point3<f64> {
        self.center
    }

    pub fn edge_length(&self) -> f64 {
        self.edge_length
    }
    pub fn half_edge_length(&self) -> f64 {
        self.edge_length / 2.0
    }

    pub fn get_lower_bound(&self) -> Point3<f64> {
        let half_edge_length = self.half_edge_length();
        self.center - Vector3::new(half_edge_length, half_edge_length, half_edge_length)
    }

    pub fn get_upper_bound(&self) -> Point3<f64> {
        let half_edge_length = self.half_edge_length();
        self.center + Vector3::new(half_edge_length, half_edge_length, half_edge_length)
    }

    pub fn diagonal(&self) -> Vector3<f64> {
        self.get_upper_bound() - self.get_lower_bound()
    }

    pub fn get_sub_cube(&self, x_half: bool, y_half: bool, z_half: bool) -> Self {
        let sub_cube_edge_length = self.half_edge_length();
        let x_sign = if x_half { 1.0 } else { -1.0 };
        let y_sign = if y_half { 1.0 } else { -1.0 };
        let z_sign = if z_half { 1.0 } else { -1.0 };

        let sub_cube_center = self.center
            + Vector3::new(
                x_sign * sub_cube_edge_length / 2.0,
                y_sign * sub_cube_edge_length / 2.0,
                z_sign * sub_cube_edge_length / 2.0,
            );

        Self::new(sub_cube_center, sub_cube_edge_length).expect("should work")
    }
}
