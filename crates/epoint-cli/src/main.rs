//! A scoped, structured logging and diagnostics system.
//!
//! # Overview

mod arguments;
mod commands;

use std::num::ParseIntError;
use std::path::{Path, PathBuf};

use crate::arguments::{Arguments, Commands};
use clap::Parser;

fn main() -> Result<(), ParseIntError> {
    tracing_subscriber::fmt::init();
    let args = Arguments::parse();

    match &args.command {
        Commands::Test {
            input_directory_path,
            output_directory_path,
        } => {
            let input_directory_path = Path::new(input_directory_path).canonicalize().unwrap();
            let output_directory_path = PathBuf::from(output_directory_path);

            commands::test::run(input_directory_path, output_directory_path);
        }
    };

    Ok(())
}
