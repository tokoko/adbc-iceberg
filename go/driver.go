package iceberg

import (
	"context"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	// Register cloud storage IO schemes (S3, GCS, Azure).
	_ "github.com/apache/iceberg-go/io/gocloud"
)

const (
	driverName    = "ADBC Iceberg Driver"
	driverVersion = "0.1.0"
)

type driverImpl struct {
	driverbase.DriverImplBase
}

func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	db, err := newDatabase(ctx, d.Base(), opts)
	if err != nil {
		return nil, err
	}
	return driverbase.NewDatabase(db), nil
}

func NewDriver(alloc memory.Allocator) adbc.Driver {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	info := driverbase.DefaultDriverInfo(driverName)
	info.RegisterInfoCode(adbc.InfoVendorName, "Apache Iceberg")
	info.RegisterInfoCode(adbc.InfoDriverName, driverName)
	info.RegisterInfoCode(adbc.InfoDriverVersion, driverVersion)

	return driverbase.NewDriver(&driverImpl{
		DriverImplBase: driverbase.NewDriverImplBase(info, alloc),
	})
}
