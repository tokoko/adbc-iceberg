/// S3 configuration shared between Iceberg and Delta readers.
#[derive(Clone, Default)]
pub struct S3Config {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

impl S3Config {
    /// Convert to Iceberg catalog properties.
    pub fn to_iceberg_props(&self) -> Vec<(String, String)> {
        let mut props = Vec::new();
        if let Some(ref v) = self.endpoint {
            props.push(("s3.endpoint".to_string(), v.clone()));
        }
        if let Some(ref v) = self.region {
            props.push(("s3.region".to_string(), v.clone()));
        }
        if let Some(ref v) = self.access_key {
            props.push(("s3.access-key-id".to_string(), v.clone()));
        }
        if let Some(ref v) = self.secret_key {
            props.push(("s3.secret-access-key".to_string(), v.clone()));
        }
        props
    }

    /// Convert to delta_kernel / object_store options.
    pub fn to_delta_options(&self) -> Vec<(String, String)> {
        let mut opts = Vec::new();
        if let Some(ref v) = self.endpoint {
            opts.push(("aws_endpoint".to_string(), v.clone()));
        }
        if let Some(ref v) = self.region {
            opts.push(("aws_region".to_string(), v.clone()));
        }
        if let Some(ref v) = self.access_key {
            opts.push(("aws_access_key_id".to_string(), v.clone()));
        }
        if let Some(ref v) = self.secret_key {
            opts.push(("aws_secret_access_key".to_string(), v.clone()));
        }
        // MinIO and other S3-compatible stores require these
        opts.push(("aws_allow_http".to_string(), "true".to_string()));
        opts.push(("aws_force_path_style".to_string(), "true".to_string()));
        opts
    }
}
