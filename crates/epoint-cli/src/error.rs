use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    EpointError(#[from] epoint::Error),
    #[error(transparent)]
    EpointIoError(#[from] epoint::io::Error),
    #[error(transparent)]
    EpointTransformError(#[from] epoint::transform::Error),

    #[error(transparent)]
    StdIoError(#[from] std::io::Error),
    #[error(transparent)]
    PolarsResult(#[from] polars::error::PolarsError),
    #[error(transparent)]
    AnyhowResult(#[from] anyhow::Error),
}
