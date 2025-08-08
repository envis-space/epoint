use crate::cli::FilterArguments;
use epoint::{Error, PointCloud};

pub fn apply_filter_arguments(
    filter_arguments: FilterArguments,
    mut point_cloud: PointCloud,
) -> Result<PointCloud, Error> {
    if let Some(min) = filter_arguments.x_min {
        point_cloud = point_cloud
            .filter_by_x_min(min)?
            .ok_or(Error::NoRemainingPoints)?;
    }
    if let Some(max) = filter_arguments.x_max {
        point_cloud = point_cloud
            .filter_by_x_max(max)?
            .ok_or(Error::NoRemainingPoints)?;
    }

    if let Some(min) = filter_arguments.y_min {
        point_cloud = point_cloud
            .filter_by_y_min(min)?
            .ok_or(Error::NoRemainingPoints)?;
    }
    if let Some(max) = filter_arguments.y_max {
        point_cloud = point_cloud
            .filter_by_y_max(max)?
            .ok_or(Error::NoRemainingPoints)?;
    }

    if let Some(min) = filter_arguments.z_min {
        point_cloud = point_cloud
            .filter_by_z_min(min)?
            .ok_or(Error::NoRemainingPoints)?;
    }
    if let Some(max) = filter_arguments.z_max {
        point_cloud = point_cloud
            .filter_by_z_max(max)?
            .ok_or(Error::NoRemainingPoints)?;
    }

    if let Some(min) = filter_arguments.spherical_range_min {
        point_cloud = point_cloud
            .filter_by_spherical_range_min(min)?
            .ok_or(Error::NoRemainingPoints)?;
    }
    if let Some(max) = filter_arguments.spherical_range_max {
        point_cloud = point_cloud
            .filter_by_spherical_range_max(max)?
            .ok_or(Error::NoRemainingPoints)?;
    }

    Ok(point_cloud)
}
