pub mod read;
mod read_impl;
pub mod write;

pub const FILE_EXTENSION_XYZ_ZST_FORMAT: &str = "xyz.zst";
pub const FILE_EXTENSION_XYZ_FORMAT: &str = "xyz";

pub const DEFAULT_XYZ_SEPARATOR: u8 = b';';
