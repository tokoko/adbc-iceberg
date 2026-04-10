package iceberg

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog"
	iceTable "github.com/apache/iceberg-go/table"
)

// partitionDescriptor is the JSON-serializable descriptor returned by ExecutePartitions
// and consumed by ReadPartition.
type partitionDescriptor struct {
	CatalogURI      string   `json:"catalog_uri"`
	TableIdent      string   `json:"table_ident"`
	SnapshotID      int64    `json:"snapshot_id"`
	FilePath        string   `json:"file_path"`
	FileFormat      string   `json:"file_format"`
	Start           int64    `json:"start"`
	Length          int64    `json:"length"`
	ProjectedFields []string `json:"projected_fields,omitempty"`
	DeleteFiles     []string `json:"delete_files,omitempty"`
}

func serializePartitionDescriptor(desc *partitionDescriptor) ([]byte, error) {
	return json.Marshal(desc)
}

func deserializePartitionDescriptor(data []byte) (*partitionDescriptor, error) {
	var desc partitionDescriptor
	if err := json.Unmarshal(data, &desc); err != nil {
		return nil, fmt.Errorf("invalid partition descriptor: %w", err)
	}
	return &desc, nil
}

// buildPartitionDescriptors converts FileScanTasks into serialized partition descriptors.
func buildPartitionDescriptors(catalogURI, tableIdent string, snapshotID int64, tasks []iceTable.FileScanTask, projectedFields []string) ([][]byte, error) {
	descriptors := make([][]byte, 0, len(tasks))

	for _, task := range tasks {
		var deleteFiles []string
		for _, df := range task.DeleteFiles {
			deleteFiles = append(deleteFiles, df.FilePath())
		}

		desc := &partitionDescriptor{
			CatalogURI:      catalogURI,
			TableIdent:      tableIdent,
			SnapshotID:      snapshotID,
			FilePath:        task.File.FilePath(),
			FileFormat:      string(task.File.FileFormat()),
			Start:           task.Start,
			Length:          task.Length,
			ProjectedFields: projectedFields,
			DeleteFiles:     deleteFiles,
		}

		data, err := serializePartitionDescriptor(desc)
		if err != nil {
			return nil, err
		}
		descriptors = append(descriptors, data)
	}
	return descriptors, nil
}

// readPartitionDescriptor reads a single partition and returns an Arrow RecordReader.
func (c *connection) readPartitionDescriptor(ctx context.Context, desc *partitionDescriptor) (array.RecordReader, error) {
	ident := catalog.ToIdentifier(desc.TableIdent)

	tbl, err := c.cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusNotFound,
			Msg:  fmt.Sprintf("table %s not found: %v", desc.TableIdent, err),
		}
	}

	var scanOpts []iceTable.ScanOption
	scanOpts = append(scanOpts, iceTable.WithSnapshotID(desc.SnapshotID))
	if len(desc.ProjectedFields) > 0 {
		scanOpts = append(scanOpts, iceTable.WithSelectedFields(desc.ProjectedFields...))
	}

	scan := tbl.Scan(scanOpts...)

	// NOTE: iceberg-go does not expose a public API to read specific FileScanTasks
	// (arrowScan.GetRecords is unexported). ToArrowRecords re-plans and reads ALL files
	// for the snapshot, not just the one described by this partition descriptor.
	// This makes ReadPartition correct but inefficient — each partition reads the full table.
	// Upstream issue needed: expose Scan.ReadTasks(ctx, []FileScanTask) on iceberg-go.
	arrowSchema, reader, err := scan.ToArrowRecords(ctx)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("failed to read partition: %v", err),
		}
	}

	rr, err := newRecordReaderFromBatchIter(c.ConnectionImplBase.Alloc, arrowSchema, reader)
	if err != nil {
		return nil, adbc.Error{Code: adbc.StatusIO, Msg: err.Error()}
	}
	return rr, nil
}

// executePartitions performs scan planning and returns partition descriptors.
func (s *statement) executePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	tbl, err := s.resolveTable(ctx)
	if err != nil {
		return nil, adbc.Partitions{}, -1, err
	}

	scan := tbl.Scan(s.buildScanOptions()...)

	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return nil, adbc.Partitions{}, -1, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("scan planning failed: %v", err),
		}
	}

	snapshotID := int64(0)
	if snap := scan.Snapshot(); snap != nil {
		snapshotID = snap.SnapshotID
	}

	descriptors, err := buildPartitionDescriptors(
		s.conn.db.uri,
		s.tableIdent(),
		snapshotID,
		tasks,
		s.projectedFields(),
	)
	if err != nil {
		return nil, adbc.Partitions{}, -1, err
	}

	schema, err := s.resolveSchema(tbl)
	if err != nil {
		return nil, adbc.Partitions{}, -1, err
	}

	partitions := adbc.Partitions{
		NumPartitions: uint64(len(descriptors)),
		PartitionIDs:  descriptors,
	}

	return schema, partitions, -1, nil
}

// executeQuery performs a full scan and returns an Arrow RecordReader.
func (s *statement) executeQuery(ctx context.Context) (array.RecordReader, int64, error) {
	tbl, err := s.resolveTable(ctx)
	if err != nil {
		return nil, -1, err
	}

	scan := tbl.Scan(s.buildScanOptions()...)

	arrowSchema, reader, err := scan.ToArrowRecords(ctx)
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("query execution failed: %v", err),
		}
	}

	rr, err := newRecordReaderFromBatchIter(s.conn.ConnectionImplBase.Alloc, arrowSchema, reader)
	if err != nil {
		return nil, -1, adbc.Error{Code: adbc.StatusIO, Msg: err.Error()}
	}
	return rr, -1, nil
}
