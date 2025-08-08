//! `epoint` is a library for processing 3D point clouds.
//!
//!
//! # Overview
//!
//!
//! # Data structure
//!
//! For serializing a point cloud, this data structure is used:
//!
//! - `point_cloud_name.tar` (uncompressed as [tarball](https://en.wikipedia.org/wiki/Tar_(computing))) or `point_cloud_name.epoint` (compressed)
//!     - `point_data.xyz` (uncompressed) or `point_data.parquet` (compressed)
//!         - mandatory fields:
//!             - `x` [f64]: X coordinate
//!             - `y` [f64]: Y coordinate
//!             - `z` [f64]: Z coordinate
//!         - optional fields: Timestamp, unique id, color
//!             - `id` [u64]: Identifier for an individual point
//!             - `frame_id` [String]: Coordinate frame the point is defined in
//!             - `timestamp_sec` [i64]: UNIX timestamp: non-leap seconds since January 1, 1970 0:00:00 UTC
//!             - `timestamp_nanosec` [u32]: Nanoseconds since the last whole non-leap second
//!             - `intensity` [f32]: Representation of the pulse return magnitude
//!                 - ROS uses [f32] and is thus preferred
//!                 - LAS uses [u16]
//!                 - PDAL uses [u16]
//!             - `sensor_translation_x` [f64]: Sensor translation X coordinate
//!             - `sensor_translation_y` [f64]: Sensor translation Y coordinate
//!             - `sensor_translation_z` [f64]: Sensor translation Z coordinate
//!             - `color_red` [u16]: Red image channel value
//!             - `color_green` [u16]: Green image channel value
//!             - `color_blue` [u16]: Blue image channel value
//!     - `info.json` (uncompressed) or `info.json.zst` (compressed)
//!         - mandatory fields:
//!         - optional fields:
//!             - `frame_id` [String]: Coordinate frame valid for all points (point data must not contain a frame_id column then)
//!     - `ecoord.json` (uncompressed) or `ecoord.json.zst` (compressed)
//!         - contains a transformation tree with validity durations
//!         - information: srid
//!         - purpose: Translate and rotate the point cloud without reading/writing the point data
//!
//! # Data structure
//!
//! To simplify the interoperability to other systems or formats, the field names try to follow:
//! - PDAL: [dimensions](https://pdal.io/en/latest/dimensions.html)
//! - ROS: [PointCloud2 Message](http://docs.ros.org/en/melodic/api/sensor_msgs/html/msg/PointCloud2.html)
//! - LAS: [Specification](https://www.asprs.org/wp-content/uploads/2019/07/LAS_1_4_r15.pdf#page=38)
//!

pub use epoint_core::{
    Error, PointCloud, PointCloudInfo, PointData, PointDataColumnType, PointDataColumns, octree,
};

pub use epoint_io as io;

pub use epoint_transform as transform;
