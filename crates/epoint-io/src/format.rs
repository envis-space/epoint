use crate::{
    FILE_EXTENSION_E57_FORMAT, FILE_EXTENSION_EPOINT_FORMAT, FILE_EXTENSION_EPOINT_TAR_FORMAT,
    FILE_EXTENSION_LAS_FORMAT, FILE_EXTENSION_LAZ_FORMAT, FILE_EXTENSION_XYZ_FORMAT,
    FILE_EXTENSION_XYZ_ZST_FORMAT,
};
use std::path::Path;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum PointCloudFormat {
    Epoint,
    EpointTar,
    E57,
    Las,
    Laz,
    Xyz,
    XyzZst,
}

impl PointCloudFormat {
    pub fn from_path(path: impl AsRef<Path>) -> Option<PointCloudFormat> {
        let path_str = path.as_ref().file_name()?.to_string_lossy().to_lowercase();

        match path_str {
            s if s.ends_with(FILE_EXTENSION_EPOINT_FORMAT) => Some(PointCloudFormat::Epoint),
            s if s.ends_with(FILE_EXTENSION_EPOINT_TAR_FORMAT) => Some(PointCloudFormat::EpointTar),
            s if s.ends_with(FILE_EXTENSION_E57_FORMAT) => Some(PointCloudFormat::E57),
            s if s.ends_with(FILE_EXTENSION_LAS_FORMAT) => Some(PointCloudFormat::Las),
            s if s.ends_with(FILE_EXTENSION_LAZ_FORMAT) => Some(PointCloudFormat::Laz),
            s if s.ends_with(FILE_EXTENSION_XYZ_FORMAT) => Some(PointCloudFormat::Xyz),
            s if s.ends_with(FILE_EXTENSION_XYZ_ZST_FORMAT) => Some(PointCloudFormat::XyzZst),
            _ => None,
        }
    }

    pub fn extension(&self) -> &'static str {
        match self {
            PointCloudFormat::Epoint => FILE_EXTENSION_EPOINT_FORMAT,
            PointCloudFormat::EpointTar => FILE_EXTENSION_EPOINT_TAR_FORMAT,
            PointCloudFormat::E57 => FILE_EXTENSION_E57_FORMAT,
            PointCloudFormat::Las => FILE_EXTENSION_LAS_FORMAT,
            PointCloudFormat::Laz => FILE_EXTENSION_LAZ_FORMAT,
            PointCloudFormat::Xyz => FILE_EXTENSION_XYZ_FORMAT,
            PointCloudFormat::XyzZst => FILE_EXTENSION_XYZ_ZST_FORMAT,
        }
    }

    pub fn is_supported_point_cloud_format(path: impl AsRef<Path>) -> bool {
        if !path.as_ref().is_file() {
            return false;
        }

        PointCloudFormat::from_path(&path).is_some()
    }
}
