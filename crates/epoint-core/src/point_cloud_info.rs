use ecoord::FrameId;

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct PointCloudInfo {
    pub frame_id: Option<FrameId>,
}

impl PointCloudInfo {
    pub fn new(frame_id: Option<FrameId>) -> Self {
        Self { frame_id }
    }
}
