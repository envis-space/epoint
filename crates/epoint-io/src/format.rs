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
    pub fn from_path(path: impl AsRef<Path>) -> Option<Self> {
        let path_str = path.as_ref().file_name()?.to_string_lossy().to_lowercase();

        match path_str {
            s if s.ends_with(FILE_EXTENSION_EPOINT_FORMAT) => Some(Self::Epoint),
            s if s.ends_with(FILE_EXTENSION_EPOINT_TAR_FORMAT) => Some(Self::EpointTar),
            s if s.ends_with(FILE_EXTENSION_E57_FORMAT) => Some(Self::E57),
            s if s.ends_with(FILE_EXTENSION_LAS_FORMAT) => Some(Self::Las),
            s if s.ends_with(FILE_EXTENSION_LAZ_FORMAT) => Some(Self::Laz),
            s if s.ends_with(FILE_EXTENSION_XYZ_FORMAT) => Some(Self::Xyz),
            s if s.ends_with(FILE_EXTENSION_XYZ_ZST_FORMAT) => Some(Self::XyzZst),
            _ => None,
        }
    }

    pub fn extension(&self) -> &'static str {
        match self {
            Self::Epoint => FILE_EXTENSION_EPOINT_FORMAT,
            Self::EpointTar => FILE_EXTENSION_EPOINT_TAR_FORMAT,
            Self::E57 => FILE_EXTENSION_E57_FORMAT,
            Self::Las => FILE_EXTENSION_LAS_FORMAT,
            Self::Laz => FILE_EXTENSION_LAZ_FORMAT,
            Self::Xyz => FILE_EXTENSION_XYZ_FORMAT,
            Self::XyzZst => FILE_EXTENSION_XYZ_ZST_FORMAT,
        }
    }

    pub fn is_supported_point_cloud_format(path: impl AsRef<Path>) -> bool {
        if !path.as_ref().is_file() {
            return false;
        }

        Self::from_path(&path).is_some()
    }
}
