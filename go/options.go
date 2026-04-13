package iceberg

// Standard ADBC option keys handled by the driver.
const (
	OptionKeyURI = "uri"
)

// Iceberg-specific database option keys.
const (
	OptionKeyCatalogName    = "adbc.iceberg.catalog.name"
	OptionKeyWarehouse      = "adbc.iceberg.warehouse"
	OptionKeyAuthToken      = "adbc.iceberg.auth.token"
	OptionKeyAuthCredential = "adbc.iceberg.auth.credential"
	OptionKeyAuthScope      = "adbc.iceberg.auth.scope"
	OptionKeyAuthURI        = "adbc.iceberg.auth.uri"
)

// S3 storage option keys.
const (
	OptionKeyS3Endpoint  = "adbc.iceberg.s3.endpoint"
	OptionKeyS3Region    = "adbc.iceberg.s3.region"
	OptionKeyS3AccessKey = "adbc.iceberg.s3.access_key"
	OptionKeyS3SecretKey = "adbc.iceberg.s3.secret_key"
)

// Iceberg-specific statement option keys.
const (
	OptionKeySnapshotID      = "adbc.iceberg.snapshot_id"
	OptionKeyAsOfTimestamp    = "adbc.iceberg.as_of_timestamp"
	OptionKeyBranch          = "adbc.iceberg.branch"
	OptionKeyStartSnapshotID = "adbc.iceberg.start_snapshot_id"
	OptionKeyBatchSize       = "adbc.iceberg.batch_size"
)
