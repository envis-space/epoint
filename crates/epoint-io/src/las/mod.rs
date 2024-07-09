use crate::Error;
use crate::Error::InvalidVersion;

pub mod read;
mod read_impl;
pub mod write;
mod write_impl;

pub const FILE_EXTENSION_LAS_FORMAT: &str = "las";
pub const FILE_EXTENSION_LAZ_FORMAT: &str = "laz";

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub enum LasVersion {
    /// LAS version 1.0 released 2003 by ASPRS.
    V1_0,
    /// LAS version 1.1 released 2005 by ASPRS.
    V1_1,
    /// LAS version 1.2 released 2008 by ASPRS.
    V1_2,
    /// LAS version 1.3 released 2010 by ASPRS.
    V1_3,
    /// LAS version 1.4 released 2013 by ASPRS.
    V1_4,
}

impl LasVersion {
    pub fn from(major: u8, minor: u8) -> Result<Self, Error> {
        match (major, minor) {
            (1, 0) => Ok(Self::V1_0),
            (1, 1) => Ok(Self::V1_1),
            (1, 2) => Ok(Self::V1_2),
            (1, 3) => Ok(Self::V1_3),
            (1, 4) => Ok(Self::V1_4),
            _ => Err(InvalidVersion { major, minor }),
        }
    }
}
