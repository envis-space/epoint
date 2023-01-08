use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("No data: {0}")]
    NoData(&'static str),
    #[error("unknown data store error")]
    ShapeMisMatch,

    #[error("Field {0} does not match type")]
    TypeMisMatch(&'static str),
    #[error("unknown data store error")]
    ColumnNameMisMatch,

    #[error("Individual points must not contain a frame_id, when the point cloud itself")]
    MultipleFrameIdDefinitions,

    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),
}
