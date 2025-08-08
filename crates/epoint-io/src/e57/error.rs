use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    EpointError(#[from] epoint_core::Error),
    #[error(transparent)]
    EpointTransformError(#[from] epoint_transform::Error),

    #[error(transparent)]
    E57Error(#[from] e57::Error),

    #[error("feature not supported")]
    NotSupported(&'static str),
    #[error("file does not contain point clouds")]
    NoPointCloudsInFile(),
    #[error(
        "number of set acquisition times '{set_acquisition_times}' does not match number of point clouds '{point_clouds}'"
    )]
    NotMatchingNumberOfAcquisitionTimes {
        set_acquisition_times: usize,
        point_clouds: usize,
    },
    #[error("no acquisition time specified")]
    NoAcquisitionTimeSpecified(),
}
