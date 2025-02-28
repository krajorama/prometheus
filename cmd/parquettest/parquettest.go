package main

import (
	"fmt"
	"io"
	"log"
	"os"

	parquet "github.com/parquet-go/parquet-go"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type TimeSeriesRow struct {
	Lbls  []ParquetLabels `parquet:"labels,list"`
	Chunk []byte          `parquet:"chunk,zstd"`
}

type ParquetLabels struct {
	Key   string `parquet:",snappy,dict"`
	Value string `parquet:",snappy,dict"`
}

// generateTimeSeriesRows returns sample time series data in TimeSeriesRow format
func generateTimeSeriesRows() []TimeSeriesRow {
	// Create time series with different labels
	rows := []TimeSeriesRow{
		{
			// First series: node_cpu_seconds_total{host="server1",region="us-west"}
			Lbls: []ParquetLabels{
				{Key: "name", Value: "node_cpu_seconds_total"},
				{Key: "host", Value: "server1"},
				{Key: "region", Value: "us-west"},
			},
			// Simple XOR encoded chunk with two samples
			Chunk: encodeChunk([]int64{1645123456000, 1645123457000}, []float64{42.5, 43.1}),
		},
		{
			// Second series: node_cpu_seconds_total{host="server2",region="us-west"}
			Lbls: []ParquetLabels{
				{Key: "name", Value: "node_cpu_seconds_total"},
				{Key: "host", Value: "server2"},
				{Key: "region", Value: "us-west"},
			},
			// Simple XOR encoded chunk with two samples
			Chunk: encodeChunk([]int64{1645123456000, 1645123457000}, []float64{42.5, 43.1}),
		},
		{
			// Third series: node_filesystem_avail_bytes{host="server1",region="us-west",mountpoint="/"}
			Lbls: []ParquetLabels{
				{Key: "name", Value: "node_filesystem_avail_bytes"},
				{Key: "host", Value: "server1"},
				{Key: "region", Value: "us-west"},
				{Key: "mountpoint", Value: "/"},
				{Key: "fstype", Value: "ext4"},
			},
			// Filesystem available bytes typically in GB range (converted to bytes)
			Chunk: encodeChunk([]int64{1645123456000, 1645123457000}, []float64{15.7 * 1e9, 15.6 * 1e9}),
		},
		{
			// Fourth series: node_filesystem_avail_bytes{host="server2",region="us-west",mountpoint="/"}
			Lbls: []ParquetLabels{
				{Key: "name", Value: "node_filesystem_avail_bytes"},
				{Key: "host", Value: "server2"},
				{Key: "region", Value: "us-west"},
				{Key: "mountpoint", Value: "/"},
				{Key: "fstype", Value: "ext4"},
			},
			// Filesystem available bytes typically in GB range (converted to bytes)
			Chunk: encodeChunk([]int64{1645123456000, 1645123457000}, []float64{22.3 * 1e9, 22.1 * 1e9}),
		},
	}

	return rows
}

// encodeChunk creates an XOR encoded chunk from timestamps and values
func encodeChunk(timestamps []int64, values []float64) []byte {
	// Create a new XOR chunk
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		panic(err)
	}

	// Add all samples to the chunk
	for i := 0; i < len(timestamps); i++ {
		appender.Append(timestamps[i], values[i])
	}

	// Return the encoded bytes
	return chunk.Bytes()
}

// testTimeSeriesRowWriter tests writing and reading TimeSeriesRow objects
func testTimeSeriesRowWriter() {
	samples := generateTimeSeriesRows()
	schema := parquet.SchemaOf(TimeSeriesRow{})

	// Write to file using generic writer
	f, _ := os.CreateTemp("", "parquet-timeseriesrow-example-")
	fileName := f.Name()
	fmt.Printf("TimeSeriesRow writer file: %s\n", fileName)

	writer := parquet.NewGenericWriter[TimeSeriesRow](f, schema)
	wroteRows, err := writer.Write(samples)
	if err != nil {
		log.Fatal(err)
	}
	_ = writer.Close()
	_ = f.Close()
	fmt.Printf("Wrote %d rows\n", wroteRows)

	// Read back using generic reader
	rf, _ := os.Open(fileName)
	reader := parquet.NewGenericReader[TimeSeriesRow](rf)

	fmt.Println("\nReading TimeSeriesRows with Generic Reader:")
	series := make([]TimeSeriesRow, wroteRows)
	readSeries, err := reader.Read(series)
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}
	fmt.Printf("Read %d rows\n", readSeries)

	for i, ts := range series {
		fmt.Printf("\tSeries %d:\n", i+1)
		fmt.Printf("\t\tLabels: ")
		for _, lbl := range ts.Lbls {
			fmt.Printf("%s=%s ", lbl.Key, lbl.Value)
		}
		fmt.Println()

		// Decode and print the chunk data
		chunk := chunkenc.NewXORChunk()
		chunk.Reset(ts.Chunk)

		iter := chunk.Iterator(nil)
		fmt.Printf("\t\tSamples: ")
		for iter.Next() == chunkenc.ValFloat {
			ts, val := iter.At()
			fmt.Printf("(%d, %f) ", ts, val)
		}
		fmt.Println()
	}
}

func main() {
	// fmt.Println("Testing Struct Writer:")
	// testStructWriter()

	// fmt.Println("\nTesting Generic Writer:")
	// testGenericWriter()

	// // Create a temporary directory for TSDB
	// fmt.Println("\nStraight forward approach using Prometheus Head")
	// testWithPrometheusHead()

	// // Test prometheus without head appender and querier
	// fmt.Println("\nTesting Prometheus without head appender and querier")
	// tsdb.TestWithoutHeadAppenderAndQuerier()

	// fmt.Println("\nTesting Custom Schema:")
	// testCustomSchema()

	fmt.Println("\nTesting TimeSeriesRow Writer:")
	testTimeSeriesRowWriter()

}
