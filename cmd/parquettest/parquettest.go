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
	Timestamp int64             `parquet:"timestamp"`
	Value     float64           `parquet:"value"`
	Labels    map[string]string `parquet:"labels"`
}

func main() {
	// Create sample data
	samples := []TimeSeries{
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

	f, _ := os.CreateTemp("", "parquet-example-")
	writer := parquet.NewWriter(f)
	for _, row := range samples {
		if err := writer.Write(row); err != nil {
			log.Fatal(err)
		}
	}
	_ = writer.Close()
	_ = f.Close()

	// Now, we can read from the file.
	rf, _ := os.Open(f.Name())
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
