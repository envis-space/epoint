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

/// GPS epoch reference timestamp (Unix time).
///
/// GPS time is defined as seconds elapsed since January 6, 1980, 00:00:00 UTC.
/// This constant represents the Unix timestamp (seconds since January 1, 1970, 00:00:00 UTC)
/// corresponding to the GPS epoch start date.
///
/// # Value
/// `315964800` seconds = 10 years, 6 days from Unix epoch to GPS epoch
///
/// ```
/// use chrono::Utc;
/// use chrono::TimeZone;
/// let base_time = Utc.with_ymd_and_hms(1980, 1, 6, 0, 0, 0).unwrap();
///
/// assert_eq!(base_time.timestamp(), 315964800);
/// ```
///
/// # Examples
/// - GPS epoch (0 GPS seconds) = Unix timestamp 315964800
/// - Current time in GPS seconds can be obtained by subtracting this constant from the Unix timestamp
///
/// # Reference
/// [GPS Time System](https://en.wikipedia.org/wiki/Global_Positioning_System#Timekeeping)
const GPS_EPOCH_REFERENCE_TIMESTAMP: i64 = 315964800;

// Adjusted GPS time offset in seconds (see: https://groups.google.com/g/lastools/c/_9TxnjoghGM)
const ADJUSTED_GPS_TIME_OFFSET: i64 = 1_000_000_000;
