package iceberg

import (
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// newRecordReaderFromBatchIter collects all record batches from an iceberg-go
// iterator and returns them as a standard array.RecordReader.
func newRecordReaderFromBatchIter(alloc memory.Allocator, schema *arrow.Schema, seq iter.Seq2[arrow.RecordBatch, error]) (array.RecordReader, error) {
	var batches []arrow.RecordBatch
	for batch, err := range seq {
		if err != nil {
			for _, b := range batches {
				b.Release()
			}
			return nil, err
		}
		batch.Retain()
		batches = append(batches, batch)
	}

	return array.NewRecordReader(schema, batches)
}
