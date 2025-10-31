mod cli;
mod commands;
mod error;
mod utility;

use anyhow::Result;

use crate::cli::{Cli, Commands};
use clap::Parser;
use nalgebra::Vector3;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match &cli.command {
        Commands::Statistics { file_path } => {
            commands::statistics::run(file_path.canonicalize()?)?;
        }
        Commands::Offset {
            input_directory,
            output_directory,
            offset,
        } => {
            let translation_offset: Vector3<f64> = Vector3::new(offset[0], offset[1], offset[2]);

            commands::offset::run(input_directory, output_directory, translation_offset)?;
        }
        Commands::Merge {
            input_directory,
            output_file,
        } => {
            commands::merge::run(input_directory, output_file)?;
        }
    };

    Ok(())
}
