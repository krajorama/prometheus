package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/parquet-go/parquet-go"
)

func main() {
	testDynamicSchema()
}

func buildSchemaForLabels(labels ...string) *parquet.Schema {
	// columns are ordered alphabetically, so I prefix the labels with "l_"
	// and the chunk with "x_" to force the order I want.
	node := parquet.Group{
		"x_chunk": parquet.Leaf(parquet.ByteArrayType),
	}
	for _, l := range labels {
		node["l_"+l] = parquet.String()
	}
	return parquet.NewSchema("metric_family", node)
}

func testDynamicSchema() {
	schema := buildSchemaForLabels("pod", "namespace")

	// Write to file using generic writer
	f, _ := os.CreateTemp("", "parquet-generic-example-")
	fileName := f.Name()
	fmt.Printf("Generic writer file: %s\n", fileName)

	writer := parquet.NewGenericWriter[any](f, schema)

	// Columns are: namespace, pod, chunk
	row := make([]parquet.Value, len(schema.Columns()))
	row[0] = parquet.ByteArrayValue([]byte("mimir-1"))
	row[1] = parquet.ByteArrayValue([]byte("ingester-4"))
	row[2] = parquet.ByteArrayValue([]byte{1, 2, 3, 4, 5})

	row2 := make([]parquet.Value, len(schema.Columns()))
	row2[0] = parquet.ByteArrayValue([]byte("mimir-2"))
	row2[1] = parquet.ByteArrayValue([]byte("ingester-5"))
	row2[2] = parquet.ByteArrayValue([]byte{5, 2, 3, 4, 5})

	rows := []parquet.Row{row, row2}
	wroteRows, err := writer.WriteRows(rows)
	if err != nil {
		log.Fatal(err)
	}
	_ = writer.Close()
	_ = f.Close()
	fmt.Printf("Wrote %d rows\n", wroteRows)

	// Read back generic rows
	rf, _ := os.Open(fileName)
	reader := parquet.NewGenericReader[any](rf)

	fmt.Println("\nReading with Generic Reader:")
	buf := make([]parquet.Row, wroteRows)

	readSeries, err := reader.ReadRows(buf)
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}
	fmt.Printf("Read %d rows\n", readSeries)
	buf = buf[:readSeries]
	for _, row := range buf {
		fmt.Printf("\t %v\n", row)
	}
}
