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
    ShapeMismatch(&'static str),

    #[error("Column `{column}` expects type `{expected}`, but received `{actual}`")]
    TypeMismatch {
        column: &'static str,
        expected: String,
        actual: String,
    },
    #[error("At column index `{0}` the column name `{1}` is expected, but received `{2}`")]
    ColumnNameMisMatch(usize, &'static str, String),
    #[error("unknown data store error")]
    ObligatoryColumn,

    #[error("column of name `{0}` already exists")]
    ColumnAlreadyExists(&'static str),

    #[error("Individual points must not contain a frame_id, when the point cloud itself")]
    MultipleFrameIdDefinitions,
    #[error(
        "Point cloud contains no frameId definition (neither in the point cloud info nor the individual points)"
    )]
    NoFrameIdDefinitions,
    #[error("Point cloud does not contain the frame id `{0}`")]
    NoFrameIdDefinition(FrameId),

    #[error("Point cloud contains no id column")]
    NoIdColumn,
    #[error("Point cloud contains no sensor translation columns")]
    NoSensorTranslationColumn,
    #[error("Point cloud contains no spherical range column")]
    NoSphericalRangeColumn,
    #[error("Point cloud contains no octant indices columns")]
    NoOctantIndicesColumns,

    #[error("Point cloud has no remaining points")]
    NoRemainingPoints,

    #[error("No row indices specified")]
    NoRowIndices,
    #[error("Row index is outside the valid range")]
    RowIndexOutsideRange,

    #[error("Lower bound exceeds upper bound")]
    LowerBoundExceedsUpperBound,
    #[error("Lower bound equals upper bound")]
    LowerBoundEqualsUpperBound,
    #[error("Invalid number")]
    InvalidNumber,
}
