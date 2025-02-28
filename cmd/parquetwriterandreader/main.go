package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/prometheus/prometheus/model/labels"
)

// ========== Writer Implementation ==========

// SeriesWriter manages Parquet file creation for Prometheus series
type SeriesWriter struct {
	baseDir     string
	fileHandles map[string]*ParquetFileHandle
	mu          sync.Mutex
}

// ParquetFileHandle manages a single Parquet file for a specific metric name
type ParquetFileHandle struct {
	metricName string
	schema     *parquet.Schema
	writer     *parquet.Writer
	file       *os.File
	labelNames []string
	labelMap   map[string]int // Maps label name to position in schema
}

// NewSeriesWriter creates a new manager for writing series to Parquet files
func NewSeriesWriter(baseDir string) *SeriesWriter {
	// Ensure directory exists
	os.MkdirAll(baseDir, 0755)

	return &SeriesWriter{
		baseDir:     baseDir,
		fileHandles: make(map[string]*ParquetFileHandle),
	}
}

// getOrCreateFileHandle gets or creates a file handle for a metric name
func (sw *SeriesWriter) getOrCreateFileHandle(metricName string, lbls labels.Labels) (*ParquetFileHandle, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if handle, exists := sw.fileHandles[metricName]; exists {
		return handle, nil
	}

	// Create schema dynamically based on the labels
	labelNames := make([]string, 0, len(lbls))
	for _, lbl := range lbls {
		if lbl.Name != "__name__" { // Skip the name label as it's used for the file
			labelNames = append(labelNames, lbl.Name)
		}
	}
	sort.Strings(labelNames) // Sort for consistency

	// Create schema definition
	schemaFields := parquet.Group{}
	for _, name := range labelNames {
		schemaFields[name] = parquet.String()
	}

	// Add timestamp and value fields
	schemaFields["timestamp"] = parquet.Leaf(parquet.ByteArrayType)

	schema := parquet.NewSchema(metricName, schemaFields)

	// Create file path
	filePath := fmt.Sprintf("%s/%s.parquet", sw.baseDir, sanitizeFileName(metricName))

	// Create file
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	// Initialize actual writer
	writer := parquet.NewWriter(file, schema)

	handle := &ParquetFileHandle{
		metricName: metricName,
		schema:     schema,
		writer:     writer,
		file:       file,
		labelNames: labelNames,
		labelMap:   make(map[string]int),
	}

	// Map each label name to its position for efficient lookup
	for i, name := range labelNames {
		handle.labelMap[name] = i
	}

	sw.fileHandles[metricName] = handle
	return handle, nil
}

// WriteSeriesSample writes a single sample to its appropriate Parquet file
func (sw *SeriesWriter) WriteSeriesSample(lbs labels.Labels, timestamp int64, value float64) error {
	metricName := lbs.Get("__name__")
	if metricName == "" {
		return fmt.Errorf("metric name not found in labels")
	}

	handle, err := sw.getOrCreateFileHandle(metricName, lbs)
	if err != nil {
		return fmt.Errorf("failed to get file handle: %w", err)
	}

	// Construct a Parquet row
	row := constructRow(handle, lbs, timestamp, value)

	// Write the row
	return handle.writer.Write(row)
}

// constructRow creates a Parquet row from Prometheus series data
func constructRow(handle *ParquetFileHandle, lbls labels.Labels, timestamp int64, value float64) parquet.Row {
	// Create a row with the right number of fields
	// We have label fields + timestamp + value
	row := make(parquet.Row, len(handle.labelNames)+2)

	// Fill in label values in the correct order according to schema
	for _, lbl := range lbls {
		if lbl.Name == "__name__" {
			continue
		}

		if pos, exists := handle.labelMap[lbl.Name]; exists {
			row[pos] = parquet.ByteArrayValue([]byte(lbl.Value))
		}
	}

	// Add timestamp and value fields at the end
	row[len(handle.labelNames)] = parquet.Int64Value(timestamp)
	row[len(handle.labelNames)+1] = parquet.DoubleValue(value)

	return row
}

// sanitizeFileName converts a metric name to a valid filename
func sanitizeFileName(name string) string {
	// Replace characters that are problematic in filenames
	name = strings.ReplaceAll(name, ":", "_")
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	return name
}

// Close closes all open file handles
func (sw *SeriesWriter) Close() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	var lastErr error
	for _, handle := range sw.fileHandles {
		if handle.writer != nil {
			if err := handle.writer.Close(); err != nil {
				lastErr = err
			}
		}
		if handle.file != nil {
			if err := handle.file.Close(); err != nil {
				lastErr = err
			}
		}
	}
	return lastErr
}

/*// ========== Reader Implementation ==========

// SeriesReader reads Parquet files and converts them to Prometheus series
type SeriesReader struct {
	baseDir string
}

// NewSeriesReader creates a new reader for Parquet files
func NewSeriesReader(baseDir string) *SeriesReader {
	return &SeriesReader{
		baseDir: baseDir,
	}
}

// Query implements a basic version of the Prometheus storage.Querier interface
func (sr *SeriesReader) Query(ctx context.Context, mint, maxt int64, matchers ...*labels.Matcher) (*MemorySeriesSet, error) {
	// Find all relevant Parquet files based on matchers
	files, err := sr.findMatchingFiles(matchers)
	if err != nil {
		return nil, err
	}

	var seriesList []storage.Series
	for _, file := range files {
		// Read series from file that match the query
		series, err := sr.readSeriesFromFile(file, mint, maxt, matchers)
		if err != nil {
			return nil, err
		}
		seriesList = append(seriesList, series...)
	}

	// Convert to a SeriesSet
	return NewMemorySeriesSet(seriesList), nil
}

// findMatchingFiles finds Parquet files matching the given matchers
func (sr *SeriesReader) findMatchingFiles(matchers []*labels.Matcher) ([]string, error) {
	var metricNameMatcher *labels.Matcher
	for _, m := range matchers {
		if m.Name == "__name__" {
			metricNameMatcher = m
			break
		}
	}

	var matchingFiles []string
	err := filepath.Walk(sr.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || !strings.HasSuffix(path, ".parquet") {
			return nil
		}

		// Extract metric name from filename
		metricName := strings.TrimSuffix(filepath.Base(path), ".parquet")
		metricName = desanitizeFileName(metricName)

		// Check if this file matches the metric name matcher
		if metricNameMatcher != nil {
			match := metricNameMatcher.Matches(metricName)
			if err != nil || !match {
				return err
			}
		}

		matchingFiles = append(matchingFiles, path)
		return nil
	})

	return matchingFiles, err
}

// readSeriesFromFile reads matching series from a Parquet file
func (sr *SeriesReader) readSeriesFromFile(filePath string, mint, maxt int64, matchers []*labels.Matcher) ([]storage.Series, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Create the Parquet reader
	reader := parquet.NewReader(file)

	// Get the schema to determine column names and types
	schema := reader.Schema()

	// Extract metric name from filename
	metricName := strings.TrimSuffix(filepath.Base(filePath), ".parquet")
	metricName = desanitizeFileName(metricName)

	// Map of label combinations to series
	seriesMap := make(map[string]*memSeries)

	// Find indices for timestamp and value columns
	var timestampIdx, valueIdx int
	var labelColumnIndices = make(map[string]int)

	// Find column indices
	for i, field := range schema.Fields() {
		if field.Name() == "timestamp" {
			timestampIdx = i
		} else if field.Name() == "value" {
			valueIdx = i
		} else {
			// This is a label column
			labelColumnIndices[field.Name()] = i
		}
	}

	// Process each row
	var row parquet.Row
	for {
		err = reader.Read(row)
		if err != nil {
			break // End of file or error
		}

		// Extract timestamp and check time range
		timestamp := row[timestampIdx].Int64()
		if timestamp < mint || timestamp > maxt {
			continue
		}

		// Extract value
		value := row[valueIdx].Double()

		// Build label set for this row
		labelSet := labels.NewBuilder(nil)
		labelSet.Set("__name__", metricName)

		for labelName, colIdx := range labelColumnIndices {
			if colIdx < len(row) && row[colIdx] != nil {
				// Convert ByteArray to string for label value
				labelValue := string(row[colIdx].ByteArray())
				labelSet.Set(labelName, labelValue)
			}
		}

		lset := labelSet.Labels()

		// Check if this series matches all matchers
		matches := true
		for _, matcher := range matchers {
			if !matchesLabels(matcher, lset) {
				matches = false
				break
			}
		}

		if !matches {
			continue
		}

		// Get or create series for this label set
		key := lset.String()
		series, ok := seriesMap[key]
		if !ok {
			series = newMemSeries(lset)
			seriesMap[key] = series
		}

		// Add sample to series
		series.addSample(timestamp, value)
	}

	// Convert map to slice
	result := make([]storage.Series, 0, len(seriesMap))
	for _, series := range seriesMap {
		result = append(result, series)
	}

	return result, nil
}

// matchesLabels checks if a label set matches a matcher
func matchesLabels(matcher *labels.Matcher, lset labels.Labels) bool {
	value := lset.Get(matcher.Name)
	match := matcher.Matches(value)
	return match
}

// desanitizeFileName converts a sanitized filename back to metric name
func desanitizeFileName(name string) string {
	// Simple reverse of sanitizeFileName
	return name
}

// memSeries is an in-memory implementation of storage.Series
type memSeries struct {
	lset   labels.Labels
	points []point
}

type point struct {
	t int64
	v float64
}

func newMemSeries(lset labels.Labels) *memSeries {
	return &memSeries{
		lset: lset,
	}
}

func (s *memSeries) addSample(t int64, v float64) {
	s.points = append(s.points, point{t: t, v: v})
}

// Labels implements storage.Series
func (s *memSeries) Labels() labels.Labels {
	return s.lset
}

// Iterator implements storage.Series
func (s *memSeries) Iterator() storage.SeriesIterator {
	// Sort points by timestamp
	sort.Slice(s.points, func(i, j int) bool {
		return s.points[i].t < s.points[j].t
	})

	return &memSeriesIterator{
		points: s.points,
		curr:   -1,
	}
}

// memSeriesIterator implements storage.SeriesIterator for memSeries
type memSeriesIterator struct {
	points []point
	curr   int
}

// Seek implements storage.SeriesIterator
func (it *memSeriesIterator) Seek(t int64) bool {
	// Find the first point with timestamp >= t
	for i := 0; i < len(it.points); i++ {
		if it.points[i].t >= t {
			it.curr = i
			return true
		}
	}
	it.curr = len(it.points)
	return false
}

// At implements storage.SeriesIterator
func (it *memSeriesIterator) At() (int64, float64) {
	if it.curr < 0 || it.curr >= len(it.points) {
		return 0, 0
	}
	p := it.points[it.curr]
	return p.t, p.v
}

// Next implements storage.SeriesIterator
func (it *memSeriesIterator) Next() bool {
	it.curr++
	return it.curr < len(it.points)
}

// Err implements storage.SeriesIterator
func (it *memSeriesIterator) Err() error {
	return nil
}

// MemorySeriesSet implements storage.SeriesSet for a slice of storage.Series
type MemorySeriesSet struct {
	series []storage.Series
	index  int
}

// NewMemorySeriesSet creates a new MemorySeriesSet
func NewMemorySeriesSet(series []storage.Series) *MemorySeriesSet {
	return &MemorySeriesSet{
		series: series,
		index:  -1,
	}
}

// Next implements storage.SeriesSet
func (ss *MemorySeriesSet) Next() bool {
	ss.index++
	return ss.index < len(ss.series)
}

// At implements storage.SeriesSet
func (ss *MemorySeriesSet) At() storage.Series {
	if ss.index < 0 || ss.index >= len(ss.series) {
		return nil
	}
	return ss.series[ss.index]
}

// Err implements storage.SeriesSet
func (ss *MemorySeriesSet) Err() error {
	return nil
}

*/ // ========== Main Example Function ==========

func main() {
	// Create a temporary directory for our test
	tempDir, err := os.MkdirTemp("", "prometheus-parquet-test")
	if err != nil {
		fmt.Printf("Failed to create temp dir: %v\n", err)
		return
	}
	defer os.RemoveAll(tempDir) // Clean up

	fmt.Printf("Using temporary directory: %s\n", tempDir)

	// Create some example series data
	now := time.Now().Unix() * 1000
	minute := int64(60 * 1000)
	hour := 60 * minute

	// ===== Writing Phase =====
	fmt.Println("Writing series to Parquet files...")
	writer := NewSeriesWriter(tempDir)

	// Write CPU metrics for different hosts and regions
	writeSeries(writer, "node_cpu_seconds_total", map[string]string{
		"host": "server1", "region": "us-west", "cpu": "0", "mode": "idle",
	}, now-24*hour, now, generateCPUValues(90, 99))

	writeSeries(writer, "node_cpu_seconds_total", map[string]string{
		"host": "server1", "region": "us-west", "cpu": "1", "mode": "idle",
	}, now-24*hour, now, generateCPUValues(85, 95))

	writeSeries(writer, "node_cpu_seconds_total", map[string]string{
		"host": "server2", "region": "us-east", "cpu": "0", "mode": "idle",
	}, now-24*hour, now, generateCPUValues(75, 85))

	// Write memory metrics for different hosts
	writeSeries(writer, "node_memory_used_bytes", map[string]string{
		"host": "server1", "region": "us-west",
	}, now-24*hour, now, generateMemoryValues(4*1024*1024*1024, 8*1024*1024*1024))

	writeSeries(writer, "node_memory_used_bytes", map[string]string{
		"host": "server2", "region": "us-east",
	}, now-24*hour, now, generateMemoryValues(2*1024*1024*1024, 6*1024*1024*1024))

	// Close writer to flush data
	if err := writer.Close(); err != nil {
		fmt.Printf("Error closing writer: %v\n", err)
		return
	}

	/*	// ===== Reading Phase =====
		fmt.Println("\nReading series from Parquet files...")
		reader := NewSeriesReader(tempDir)

		// Example 1: Query for CPU metrics from server1
		fmt.Println("\nQuery 1: CPU metrics from server1")
		queryWithMatchers(reader, now-24*hour, now,
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "node_cpu_seconds_total"),
			labels.MustNewMatcher(labels.MatchEqual, "host", "server1"))

		// Example 2: Query for all us-west region metrics
		fmt.Println("\nQuery 2: All metrics from us-west region")
		queryWithMatchers(reader, now-24*hour, now,
			labels.MustNewMatcher(labels.MatchRegexp, "__name__", "node_.*"),
			labels.MustNewMatcher(labels.MatchEqual, "region", "us-west"))

		// Example 3: Query for memory metrics
		fmt.Println("\nQuery 3: Memory metrics for all hosts")
		queryWithMatchers(reader, now-24*hour, now,
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "node_memory_used_bytes"))

		fmt.Println("Done!")
	*/
}

// Helper functions for the example

// writeSeries writes a series with generated values
func writeSeries(writer *SeriesWriter, name string, lblMap map[string]string, start, end int64, values []float64) {
	// Create label set
	builder := labels.NewBuilder(nil)
	builder.Set("__name__", name)
	for k, v := range lblMap {
		builder.Set(k, v)
	}
	lbls := builder.Labels()

	// Time interval between points
	interval := (end - start) / int64(len(values))

	// Write each data point
	for i, val := range values {
		ts := start + (int64(i) * interval)
		if err := writer.WriteSeriesSample(lbls, ts, val); err != nil {
			fmt.Printf("Error writing sample: %v\n", err)
		}
	}
}

// generateCPUValues generates realistic CPU idle values
func generateCPUValues(min, max float64) []float64 {
	count := 24 // One value per hour for a day
	values := make([]float64, count)

	range_ := max - min
	for i := 0; i < count; i++ {
		// Simple simulation with some randomness and a day/night pattern
		timeOfDay := float64(i%24) / 24.0
		// Values lower at "business hours"
		dayFactor := 0.5 - 0.5*timeOfDay + 0.5*timeOfDay*timeOfDay
		values[i] = min + range_*dayFactor*float64(7+i%5)/10.0
	}

	return values
}

// generateMemoryValues generates realistic memory usage values
func generateMemoryValues(min, max float64) []float64 {
	count := 24 // One value per hour for a day
	values := make([]float64, count)

	range_ := max - min
	for i := 0; i < count; i++ {
		// Memory often increases during the day and drops after maintenance/restarts
		timeOfDay := float64(i%24) / 24.0
		memPattern := timeOfDay - 0.1*float64(i%3)
		if memPattern < 0 {
			memPattern = 0
		}
		values[i] = min + range_*memPattern
	}

	return values
}

/*// queryWithMatchers performs a query and prints results
func queryWithMatchers(reader *SeriesReader, start, end int64, matchers ...*labels.Matcher) {
	// Print query details
	fmt.Print("Query: ")
	for i, m := range matchers {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Printf("%s%s%q", m.Name, matcherTypeToString(m.Type), m.Value)
	}
	fmt.Println()

	// Execute query
	seriesSet, err := reader.Query(context.Background(), start, end, matchers...)
	if err != nil {
		fmt.Printf("Query error: %v\n", err)
		return
	}

	// Print results
	seriesCount := 0
	sampleCount := 0

	for seriesSet.Next() {
		series := seriesSet.At()
		seriesCount++

		fmt.Printf("- Series: %s\n", series.Labels())

		// Print first few samples
		it := series.Iterator()
		pointsInSeries := 0
		for it.Next() && pointsInSeries < 3 {
			ts, val := it.At()
			fmt.Printf("  %s: %f\n",
				time.Unix(ts/1000, 0).Format("2006-01-02 15:04:05"),
				val)
			pointsInSeries++
			sampleCount++
		}

		// Count remaining samples without printing
		for it.Next() {
			sampleCount++
			pointsInSeries++
		}

		if pointsInSeries > 3 {
			fmt.Printf("  ... and %d more samples\n", pointsInSeries-3)
		}
	}

	fmt.Printf("Found %d series with %d total samples\n", seriesCount, sampleCount)
}*/

// matcherTypeToString converts a matcher type to a string operator
func matcherTypeToString(t labels.MatchType) string {
	switch t {
	case labels.MatchEqual:
		return "="
	case labels.MatchNotEqual:
		return "!="
	case labels.MatchRegexp:
		return "=~"
	case labels.MatchNotRegexp:
		return "!~"
	default:
		return "?"
	}
}
