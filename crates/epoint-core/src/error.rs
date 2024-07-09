use ecoord::FrameId;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    EcoordError(#[from] ecoord::Error),
    #[error(transparent)]
    Polars(#[from] polars::error::PolarsError),

    #[error("No data: {0}")]
    NoData(&'static str),
    #[error("Lengths don't match: {0}")]
    ShapeMisMatch(&'static str),

    #[error("Field {0} does not match type")]
    TypeMisMatch(&'static str),
    #[error("At column index `{0}` the column name `{1}` is expected, but received `{2}`")]
    ColumnNameMisMatch(usize, &'static str, String),
    #[error("unknown data store error")]
    ObligatoryColumn,

    #[error("column of name `{0}` already exists")]
    ColumnAlreadyExists(&'static str),

    #[error("Individual points must not contain a frame_id, when the point cloud itself")]
    MultipleFrameIdDefinitions,
    #[error("Point cloud contains no frameId definition (neither in the point cloud info nor the individual points)")]
    NoFrameIdDefinitions,
    #[error("Point cloud does not contain the frame id `{0}`")]
    NoFrameIdDefinition(FrameId),

    #[error("Point cloud contains no id column")]
    NoIdColumn,
    #[error("Point cloud contains no id column")]
    NoBeamOriginColumn,

    #[error("No row indices specified")]
    NoRowIndices,
    #[error("No row indices specified")]
    RowIndexOutsideRange,

    #[error("No row indices specified")]
    LowerBoundExceedsUpperBound,
    #[error("No row indices specified")]
    LowerBoundEqualsUpperBound,
    #[error("path is not a directory")]
    InvalidNumber,
}
