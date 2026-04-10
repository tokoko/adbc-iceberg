package iceberg

import (
	"context"
	"fmt"
	"net/url"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
)

type database struct {
	driverbase.DatabaseImplBase

	uri         string
	catalogName string
	authToken   string
	credential  string
	authScope   string
	authURI     string

	s3Endpoint  string
	s3Region    string
	s3AccessKey string
	s3SecretKey string
}

func newDatabase(ctx context.Context, driver *driverbase.DriverImplBase, opts map[string]string) (*database, error) {
	base, err := driverbase.NewDatabaseImplBase(ctx, driver)
	if err != nil {
		return nil, err
	}
	db := &database{
		DatabaseImplBase: base,
		catalogName:      "default",
	}
	for k, v := range opts {
		if err := db.SetOption(k, v); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func (db *database) SetOption(key, value string) error {
	switch key {
	case OptionKeyURI:
		db.uri = value
	case OptionKeyCatalogName:
		db.catalogName = value
	case OptionKeyAuthToken:
		db.authToken = value
	case OptionKeyAuthCredential:
		db.credential = value
	case OptionKeyAuthScope:
		db.authScope = value
	case OptionKeyAuthURI:
		db.authURI = value
	case OptionKeyS3Endpoint:
		db.s3Endpoint = value
	case OptionKeyS3Region:
		db.s3Region = value
	case OptionKeyS3AccessKey:
		db.s3AccessKey = value
	case OptionKeyS3SecretKey:
		db.s3SecretKey = value
	default:
		return db.DatabaseImplBase.SetOption(key, value)
	}
	return nil
}

func (db *database) GetOption(key string) (string, error) {
	switch key {
	case OptionKeyURI:
		return db.uri, nil
	case OptionKeyCatalogName:
		return db.catalogName, nil
	default:
		return db.DatabaseImplBase.GetOption(key)
	}
}

func (db *database) Open(ctx context.Context) (adbc.Connection, error) {
	if db.uri == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "uri is required",
		}
	}

	cat, err := db.openCatalog(ctx)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("failed to open catalog: %v", err),
		}
	}

	conn := newConnection(db, cat)
	return driverbase.NewConnectionBuilder(conn).
		WithDbObjectsEnumerator(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		Connection(), nil
}

func (db *database) openCatalog(ctx context.Context) (catalog.Catalog, error) {
	var opts []rest.Option

	if db.authToken != "" {
		opts = append(opts, rest.WithOAuthToken(db.authToken))
	}
	if db.credential != "" {
		opts = append(opts, rest.WithCredential(db.credential))
	}
	if db.authScope != "" {
		opts = append(opts, rest.WithScope(db.authScope))
	}
	if db.authURI != "" {
		u, err := url.Parse(db.authURI)
		if err != nil {
			return nil, fmt.Errorf("invalid auth URI: %w", err)
		}
		opts = append(opts, rest.WithAuthURI(u))
	}

	props := make(iceberg.Properties)
	if db.s3Endpoint != "" {
		props[iceio.S3EndpointURL] = db.s3Endpoint
	}
	if db.s3Region != "" {
		props[iceio.S3Region] = db.s3Region
	}
	if db.s3AccessKey != "" {
		props[iceio.S3AccessKeyID] = db.s3AccessKey
	}
	if db.s3SecretKey != "" {
		props[iceio.S3SecretAccessKey] = db.s3SecretKey
	}
	if len(props) > 0 {
		opts = append(opts, rest.WithAdditionalProps(props))
	}

	return rest.NewCatalog(ctx, db.catalogName, db.uri, opts...)
}

func (db *database) Close() error {
	return nil
}
