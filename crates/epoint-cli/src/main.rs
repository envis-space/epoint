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
        Commands::Transform {
            input_directory,
            output_directory,
            translation,
            frame_id,
            format,
        } => {
            let translation_vector: Vector3<f64> =
                Vector3::new(translation[0], translation[1], translation[2]);

            commands::transform::run(
                input_directory,
                output_directory,
                translation_vector,
                frame_id,
                format.to_epoint_format(),
            )?;
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
