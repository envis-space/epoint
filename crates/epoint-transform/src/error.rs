use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    EpointError(#[from] epoint_core::Error),
}
