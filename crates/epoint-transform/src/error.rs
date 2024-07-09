use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    EcoordError(#[from] ecoord::Error),
    #[error(transparent)]
    EpointError(#[from] epoint_core::Error),
    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),

    #[error("path is not a directory")]
    ContainsNoPoints,
    #[error("path is not a directory")]
    ContainsColors,
    #[error("path is not a directory")]
    InvalidNumber,
    #[error("path is not a directory")]
    DifferentPointCloudInfos,
}
