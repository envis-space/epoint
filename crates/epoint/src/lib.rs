//! `epoint` is a crate for representing point clouds with the extension `.epoi`.
//!
//!
//! # Overview
//!
//!
//! # Data structure
//!
//! For serializing a point cloud, this data structure is used:
//!
//! - `point_cloud_name` (directory) or `point_cloud_name.epoi` (single file as [tarball](https://en.wikipedia.org/wiki/Tar_(computing)))
//!     - `point_data.xyz` (uncompressed) or `point_data.parquet` (compressed)
//!         - mandatory fields:
//!             - `x`: [f64](f64)
//!             - `y`: [f64](f64)
//!             - `z`: [f64](f64)
//!         - optional fields: timestamp, unique id, color
//!             - `frame_id`: [String](String)
//!             - `timestamp_sec`: [i64](i64) (UNIX timestamp: non-leap seconds since January 1, 1970 0:00:00 UTC)
//!             - `timestamp_nanosec`: [u32](u32) (nanoseconds since the last whole non-leap second)
//!             - `intensity`: [f32](f32)
//!                 - ROS uses [f32](f32) and is thus preferred
//!                 - LAS uses [u16](u16)
//!     - `info.json`
//!         - mandatory fields:
//!         - optional fields: indices?
//!             - `frame_id`: [String](String)
//!     - `frames.json`
//!         - contains a transformation tree with validity durations
//!         - kind of similar to a geopose
//!         - information: srid
//!         - purpose: translate and rotate the point cloud without reading/writing the point data
//!

pub use epoint_core::{Error, PointCloud, PointCloudInfo, PointDataColumnNames, PointDataColumns};

pub use epoint_io as io;

pub use epoint_transform as transform;
