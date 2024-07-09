use ecoord::FrameId;
use epoint_core::PointCloudInfo;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpointInfoDocument {
    pub frame_id: Option<String>,
}

impl Default for EpointInfoDocument {
    fn default() -> Self {
        Self::new()
    }
}

impl EpointInfoDocument {
    pub fn new() -> Self {
        Self { frame_id: None }
    }

    pub fn with_frame_id(mut self, frame_id: Option<FrameId>) -> Self {
        self.frame_id = frame_id.map(|f| f.into());
        self
    }
}

impl From<EpointInfoDocument> for PointCloudInfo {
    fn from(item: EpointInfoDocument) -> Self {
        PointCloudInfo {
            frame_id: item.frame_id.map(|f| f.into()),
        }
    }
}
