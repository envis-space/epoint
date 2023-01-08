#[cfg(test)]
mod point_cloud_construction_test {

    use chrono::{DateTime, TimeZone, Utc};
    use ecoord::ReferenceFrames;
    use epoint_core::{PointCloud, PointCloudInfo, PointDataColumns};
    use nalgebra::Point3;

    #[test]
    fn test_basic_point_cloud() {
        let point = Point3::<f64>::new(0.0, 0.0, 0.0);
        let point_cloud = vec![point];

        let frame_id: Vec<String> = vec!["test_frame".into(); point_cloud.len()];
        let timestamp: Vec<DateTime<Utc>> =
            vec![Utc.timestamp_opt(61, 0).unwrap(); point_cloud.len()];

        let point_data =
            PointDataColumns::new(vec![point], Some(frame_id), Some(timestamp), None).unwrap();
        let _point_cloud = PointCloud::new(
            point_data,
            PointCloudInfo::default(),
            ReferenceFrames::default(),
        )
        .unwrap();
    }

    #[test]
    fn test_fail_with_ambiguous_frame_ids() {
        let point = Point3::<f64>::new(0.0, 0.0, 0.0);
        let point_cloud = vec![point];

        let frame_id: Vec<String> = vec!["test_frame".into(); point_cloud.len()];
        let timestamp: Vec<DateTime<Utc>> =
            vec![Utc.timestamp_opt(61, 0).unwrap(); point_cloud.len()];

        let point_data =
            PointDataColumns::new(vec![point], Some(frame_id), Some(timestamp), None).unwrap();
        let point_info = PointCloudInfo::new(Some("another_frame_id".into()));
        let point_cloud = PointCloud::new(point_data, point_info, ReferenceFrames::default());

        assert!(point_cloud.is_err())
    }
}
