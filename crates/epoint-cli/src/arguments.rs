use clap::{Parser, Subcommand};

#[derive(Parser)]
#[clap(author, version, about, long_about = None, propagate_version = true)]
pub struct Arguments {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Compute some statistics about the dataset
    Statistics {
        /// Input directory
        #[clap(short, long)]
        file_path: String,
    },

    /// Compute some statistics about the dataset
    Offset {
        /// Input directory
        #[clap(short, long)]
        input_directory: String,

        /// Path to the output directory
        #[clap(short, long)]
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
        #[clap(short, long)]
        input_directory: String,

        /// Path to the output file
        #[clap(short, long)]
        output_file: String,
    },

    /// Run some tests
    Test {
        /// Input directory
        #[clap(long)]
        input_path: String,

        /// Output directory
        #[clap(long)]
        output_directory_path: String,
    },
}
