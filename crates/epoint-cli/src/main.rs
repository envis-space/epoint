mod cli;
mod commands;
mod error;
mod utility;

use anyhow::Result;
use std::path::{Path, PathBuf};

use crate::cli::{Cli, Commands};
use clap::Parser;
use nalgebra::Vector3;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match &cli.command {
        Commands::Statistics { file_path } => {
            let file_path = Path::new(file_path).canonicalize()?;

            commands::statistics::run(file_path)?;
        }
        Commands::Offset {
            input_directory,
            output_directory,
            offset,
        } => {
            let input_directory = PathBuf::from(input_directory);
            let output_directory = PathBuf::from(output_directory);
            let translation_offset: Vector3<f64> = Vector3::new(offset[0], offset[1], offset[2]);

            commands::offset::run(input_directory, output_directory, translation_offset)?;
        }
        Commands::Merge {
            input_directory,
            output_file,
        } => {
            let input_directory = PathBuf::from(input_directory);
            let output_file = PathBuf::from(output_file);

            commands::merge::run(input_directory, output_file)?;
        }
        Commands::Test {
            input_path,
            output_directory_path,
        } => {
            let input_path = PathBuf::from(input_path);
            let output_directory_path = PathBuf::from(output_directory_path);

            commands::test::run(input_path, output_directory_path)?;
        }
    };

    Ok(())
}
