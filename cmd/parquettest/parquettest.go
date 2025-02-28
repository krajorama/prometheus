package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"time"

	parquet "github.com/parquet-go/parquet-go"
	"github.com/prometheus/common/promslog"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// TimeSeries represents a single time series data point with labels
type TimeSeries struct {
	Timestamp int64             `parquet:",delta"`
	Value     float64           `parquet:",zstd"`
	Labels    map[string]string `parquet:"labels"`
}

type TimeSeriesRow struct {
	Lbls  []ParquetLabels `parquet:"labels,list"`
	Chunk []byte          `parquet:"chunk,zstd"`
}

type ParquetLabels struct {
	Key   string `parquet:",snappy,dict"`
	Value string `parquet:",snappy,dict"`
}

// getSampleData returns sample time series data for testing
func getSampleData() []TimeSeries {
	return []TimeSeries{
		// server 1
		{
			Timestamp: 1645123456,
			Value:     42.5,
			Labels: map[string]string{
				"name":   "node_cpu_seconds_total",
				"host":   "server1",
				"region": "us-west",
			},
		},
		{
			Timestamp: 1645123457,
			Value:     43.1,
			Labels: map[string]string{
				"name":   "node_cpu_seconds_total",
				"host":   "server1",
				"region": "us-west",
			},
		},
		// server 2
		{
			Timestamp: 1645123456,
			Value:     42.5,
			Labels: map[string]string{
				"name":   "node_cpu_seconds_total",
				"host":   "server2",
				"region": "us-west",
			},
		},
		{
			Timestamp: 1645123457,
			Value:     43.1,
			Labels: map[string]string{
				"name":   "node_cpu_seconds_total",
				"host":   "server2",
				"region": "us-west",
			},
		},
	}
}

func testGenericWriter() {
	samples := getSampleData()
	schema := parquet.SchemaOf(TimeSeries{})

	// Write to file using generic writer
	f, _ := os.CreateTemp("", "parquet-generic-example-")
	fileName := f.Name()
	fmt.Printf("Generic writer file: %s\n", fileName)

	writer := parquet.NewGenericWriter[TimeSeries](f, schema)
	wroteRows, err := writer.Write(samples)
	if err != nil {
		log.Fatal(err)
	}
	_ = writer.Close()
	_ = f.Close()
	fmt.Printf("Wrote %d rows\n", wroteRows)

	// Read back using generic reader
	rf, _ := os.Open(fileName)
	reader := parquet.NewGenericReader[TimeSeries](rf)

	fmt.Println("\nReading with Generic Reader:")
	series := make([]TimeSeries, wroteRows)
	readSeries, err := reader.Read(series)
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}
	fmt.Printf("Read %d rows\n", readSeries)
	for _, ts := range series {
		fmt.Printf("\t Timestamp: %d, Value %f, Labels %v \n", ts.Timestamp, ts.Value, ts.Labels)
	}
}

func testStructWriter() {
	samples := getSampleData()

	f, _ := os.CreateTemp("", "parquet-example-")
	fileName := f.Name()
	fmt.Printf("Struct writer file: %s\n", fileName)

	writer := parquet.NewWriter(f)
	for _, row := range samples {
		if err := writer.Write(row); err != nil {
			log.Fatal(err)
		}
	}
	_ = writer.Close()
	_ = f.Close()

	// Now, we can read from the file.
	rf, _ := os.Open(fileName)
	pf := parquet.NewReader(rf)
	series := make([]TimeSeries, 0)
	for {
		var ts TimeSeries
		err := pf.Read(&ts)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		series = append(series, ts)
	}
	fmt.Printf("Read %d rows\n", len(series))
	for _, ts := range series {
		fmt.Printf("\t Timestamp: %d, Value %f, Labels %v \n", ts.Timestamp, ts.Value, ts.Labels)
	}
}

func testCustomSchema() {
	fmt.Println("\nTesting Custom Schema Definition:")

	// Define a custom schema with explicit columns
	// schema := parquet.NewSchema("node_cpu_seconds_total", parquet.Group{
	// 	"host":   parquet.String(),
	// 	"region": parquet.String(),
	// 	"chunk":  parquet.Leaf(parquet.ByteArrayType),
	// })
	// Create sample data as rows
	rows := []parquet.Row{
		{
			parquet.ByteArrayValue([]byte("server1")),   // host field
			parquet.ByteArrayValue([]byte("us-west-1")), // region field
			parquet.ByteArrayValue([]byte{1, 2, 3}),     // chunk field
		},
		{
			parquet.ByteArrayValue([]byte("server2")),   // host field
			parquet.ByteArrayValue([]byte("us-west-1")), // region field
			parquet.ByteArrayValue([]byte{1, 2, 3}),     // chunk field
		},
	}

	// Write to file
	f, _ := os.CreateTemp("", "parquet-custom-schema-*.parquet")
	fileName := f.Name()
	fmt.Printf("Custom schema file: %s\n", fileName)

	// Pass the schema to the writer
	writer := parquet.NewWriter(f)
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			log.Fatal(err)
		}
	}

	if err := writer.Close(); err != nil {
		log.Fatal(err)
	}
	f.Close()

	// // Read back the data
	// rf, _ := os.Open(fileName)
	// reader := parquet.NewReader(rf)

	// fmt.Println("\nReading with custom schema:")
	// for {
	// 	// Create an empty row to read into
	// 	var row parquet.Row
	// 	row, err := reader.Read()
	// 	if err == io.EOF {
	// 		break
	// 	}
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}

	// 	// Access values by index - now correctly matching our schema
	// 	host := row[0].String()
	// 	region := row[1].String()
	// 	chunk := row[2].ByteArray()

	// 	fmt.Printf("\tHost: %s, Region: %s, Chunk: %v\n",
	// 		host, region, chunk)
	// }
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

	fmt.Println("\nTesting TimeSeriesRow Writer:")
	testTimeSeriesRowWriter()

	// // Create a temporary directory for TSDB
	// fmt.Println("\nStraight forward approach using Prometheus Head")
	// testWithPrometheusHead()

	// // Test prometheus without head appender and querier
	// fmt.Println("\nTesting Prometheus without head appender and querier")
	// tsdb.TestWithoutHeadAppenderAndQuerier()

	// fmt.Println("\nTesting Custom Schema:")
	// testCustomSchema()
}

func testWithPrometheusHead() {
	dir, err := os.MkdirTemp("", "tsdb_test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	// Create TSDB with default options
	opts := tsdb.DefaultOptions()
	db, err := tsdb.Open(dir, promslog.NewNopLogger(), nil, opts, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Create some sample metrics
	ctx := context.Background()
	app := db.Appender(ctx)

	// Create labels for our series
	lset := labels.FromStrings(
		"__name__", "test_metric",
		"instance", "localhost:9090",
		"job", "prometheus",
	)

	// Add samples
	var ref storage.SeriesRef
	now := time.Now().UnixNano() / int64(time.Millisecond)

	for i := 0; i < 10; i++ {
		ts := now + int64(i*1000) // Add samples every second
		ref, err = app.Append(ref, lset, ts, float64(i*100))
		if err != nil {
			panic(err)
		}
	}

	if err := app.Commit(); err != nil {
		panic(err)
	}

	// Query back the data
	querier, err := db.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		panic(err)
	}
	defer querier.Close()

	// Select our series
	seriesSet := querier.Select(ctx, true, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric"))

	// Convert to Parquet format
	var series []TimeSeries

	for seriesSet.Next() {
		s := seriesSet.At()
		iter := s.Iterator(nil)
		lbls := s.Labels()
		labelMap := make(map[string]string)
		lbls.Range(func(l labels.Label) {
			labelMap[l.Name] = l.Value
		})

		for iter.Next() == chunkenc.ValFloat {
			t, v := iter.At()
			series = append(series, TimeSeries{
				Timestamp: t,
				Value:     v,
				Labels:    labelMap,
			})
		}
	}

	if seriesSet.Err() != nil {
		panic(seriesSet.Err())
	}

	// Write to Parquet file
	f, err := os.CreateTemp("", "prometheus-parquet-*.parquet")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	schema := parquet.SchemaOf(TimeSeries{})
	writer := parquet.NewGenericWriter[TimeSeries](f, schema)

	n, err := writer.Write(series)
	if err != nil {
		panic(err)
	}

	err = writer.Close()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Wrote %d series to %s\n", n, f.Name())

	rf, _ := os.Open(f.Name())
	pf := parquet.NewReader(rf)
	series = make([]TimeSeries, 0)
	for {
		var ts TimeSeries
		err := pf.Read(&ts)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		series = append(series, ts)
	}
	fmt.Printf("Read %d rows\n", len(series))
	for _, ts := range series {
		fmt.Printf("\t Timestamp: %d, Value %f, Labels %v \n", ts.Timestamp, ts.Value, ts.Labels)
	}
}
