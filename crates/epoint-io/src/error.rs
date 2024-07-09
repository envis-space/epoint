use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    EpointError(#[from] epoint_core::Error),
    #[error(transparent)]
    EcoordIo(#[from] ecoord::io::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Parsing(#[from] serde_json::Error),
    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),
    #[error(transparent)]
    Las(#[from] las::Error),
    #[error(transparent)]
    StdSystemTimeError(#[from] std::time::SystemTimeError),

    #[error("file extension is invalid")]
    NoDirectoryPath(),
    #[error("file extension is invalid")]
    NoFileExtension(),
    #[error("file extension is invalid")]
    FileNotFound(String),

    #[error("file extension `{0}` is invalid")]
    InvalidFileExtension(String),
    #[error("invalid version of major={major} and minor={minor}")]
    InvalidVersion { major: u8, minor: u8 },

    #[error("file extension is invalid")]
    PointDataFileNotFound(),
}
