pub mod read;

// TODO: not make public
pub mod read_impl;
pub mod write;
// TODO: not make public
mod documents;
pub mod write_impl;

pub const FILE_EXTENSION_EPOINT_COMPRESSED: &str = "epoint";
pub const FILE_EXTENSION_EPOINT_UNCOMPRESSED: &str = "tar";

pub const FILE_NAME_POINT_DATA_COMPRESSED: &str = "point_data.parquet";
pub const FILE_NAME_POINT_DATA_UNCOMPRESSED: &str = "point_data.xyz";
pub const FILE_NAME_INFO_COMPRESSED: &str = "info.json.zst";
pub const FILE_NAME_INFO_UNCOMPRESSED: &str = "info.json";
pub const FILE_NAME_ECOORD_COMPRESSED: &str = "ecoord.json.zst";
pub const FILE_NAME_ECOORD_UNCOMPRESSED: &str = "ecoord.json";
