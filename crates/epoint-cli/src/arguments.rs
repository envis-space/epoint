use clap::{Parser, Subcommand};

#[derive(Parser)]
#[clap(author, version, about, long_about = None, propagate_version = true)]
pub struct Arguments {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run some tests
    Test {
        /// Input directory
        #[clap(long)]
        input_directory_path: String,

        /// Output directory
        #[clap(long)]
        output_directory_path: String,
    },
}
