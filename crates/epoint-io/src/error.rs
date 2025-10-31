use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    EpointIoE57Error(#[from] crate::e57::error::Error),

    #[error(transparent)]
    EpointError(#[from] epoint_core::Error),
    #[error(transparent)]
    EcoordError(#[from] ecoord::Error),
    #[error(transparent)]
    EcoordIoError(#[from] ecoord::io::Error),
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
    NoFileName(),
    #[error("file extension is invalid")]
    FileNotFound(String),
    #[error("file extension is invalid")]
    FormatNotSupported(String),

    #[error("file extension `{0}` is invalid")]
    InvalidFileExtension(String),
    #[error("file extension `{0}` is unknown")]
    UnknownFileExtension(String),
    #[error("invalid version of major={major} and minor={minor}")]
    InvalidVersion { major: u8, minor: u8 },

    #[error("file extension is invalid")]
    PointDataFileNotFound(),
}
