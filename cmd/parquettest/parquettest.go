package main

import (
	"fmt"
	"io"
	"log"
	"os"

	parquet "github.com/parquet-go/parquet-go"
)

// TimeSeries represents a single time series data point with labels
type TimeSeries struct {
	Timestamp int64             `parquet:",delta"`
	Value     float64           `parquet:",zstd"`
	Labels    map[string]string `parquet:"labels"`
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

func main() {
	fmt.Println("Testing Struct Writer:")
	testStructWriter()

	fmt.Println("\nTesting Generic Writer:")
	testGenericWriter()
}
