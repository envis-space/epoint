use clap::{Args, Parser, Subcommand, ValueEnum, ValueHint};

#[derive(Parser)]
#[clap(author, version, about, long_about = None, propagate_version = true)]
pub struct Cli {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Compute some statistics about the dataset
    Statistics {
        /// Input directory
        #[clap(short, long, value_hint = ValueHint::FilePath)]
        file_path: String,
    },

    /// Compute some statistics about the dataset
    Offset {
        /// Input directory
        #[clap(short, long, value_hint = ValueHint::DirPath)]
        input_directory: String,

        /// Path to the output directory
        #[clap(short, long, value_hint = ValueHint::DirPath)]
        output_directory: String,

        /// Offset point cloud
        #[clap(
            long,
            required = true,
            number_of_values = 3,
            allow_hyphen_values = true
        )]
        offset: Vec<f64>,
    },

    /// Merge point clouds
    Merge {
        /// Input directory
        #[clap(short, long, value_hint = ValueHint::DirPath)]
        input_directory: String,

        /// Path to the output file
        #[clap(short, long, value_hint = ValueHint::DirPath)]
        output_file: String,
    },

    /// Run some tests
    Test {
        /// Input directory
        #[clap(long, value_hint = ValueHint::DirPath)]
        input_path: String,

        /// Output directory
        #[clap(long, value_hint = ValueHint::DirPath)]
        output_directory_path: String,
    },
}

#[derive(Args, Debug, Clone, Copy, PartialEq)]
pub struct FilterArguments {
    /// Minimum value for X coordinate (inclusive)
    #[clap(long = "filter-by-x-min")]
    pub x_min: Option<f64>,

    /// Maximum value for X coordinate (inclusive)
    #[clap(long = "filter-by-x-max")]
    pub x_max: Option<f64>,

    /// Minimum value for Y coordinate (inclusive)
    #[clap(long = "filter-by-y-min")]
    pub y_min: Option<f64>,

    /// Maximum value for Y coordinate (inclusive)
    #[clap(long = "filter-by-y-max")]
    pub y_max: Option<f64>,

    /// Minimum value for Z coordinate (inclusive)
    #[clap(long = "filter-by-z-min")]
    pub z_min: Option<f64>,

    /// Maximum value for Z coordinate (inclusive)
    #[clap(long = "filter-by-z-max")]
    pub z_max: Option<f64>,

    /// Minimum distance for spherical range filtering (inclusive)
    #[clap(long = "filter-by-spherical-range-min")]
    pub spherical_range_min: Option<f64>,

    /// Maximum distance for spherical range filtering (inclusive)
    #[clap(long = "filter-by-spherical-range-max")]
    pub spherical_range_max: Option<f64>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, ValueEnum)]
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
    pub fn to_epoint_format(&self) -> epoint::io::PointCloudFormat {
        match self {
            PointCloudFormat::Epoint => epoint::io::PointCloudFormat::Epoint,
            PointCloudFormat::EpointTar => epoint::io::PointCloudFormat::EpointTar,
            PointCloudFormat::E57 => epoint::io::PointCloudFormat::E57,
            PointCloudFormat::Las => epoint::io::PointCloudFormat::Las,
            PointCloudFormat::Laz => epoint::io::PointCloudFormat::Laz,
            PointCloudFormat::Xyz => epoint::io::PointCloudFormat::Xyz,
            PointCloudFormat::XyzZst => epoint::io::PointCloudFormat::XyzZst,
        }
    }
}
