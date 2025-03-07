// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/columnar"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/util/annotations"
)

type columnarQuerier struct {
	dir        string
	closed     bool
	mint, maxt int64

	includeLabels []string

	ix *columnar.Index

	parquetFile *os.File
}

func NewColumnarQuerier(dir string, ix *columnar.Index, mint, maxt int64, includeLabels []string) (*columnarQuerier, error) {
	return &columnarQuerier{
		dir:           dir,
		mint:          mint,
		maxt:          maxt,
		includeLabels: includeLabels,
		ix:            ix,
	}, nil
}

func (q *columnarQuerier) LabelValues(ctx context.Context, name string, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	if name == "instance" {
		return []string{"instance1", "instance2"}, nil, nil
	} else {
		return []string{"job1", "job2"}, nil, nil
	}
}

func (q *columnarQuerier) LabelNames(ctx context.Context, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return []string{"instance", "job"}, nil, nil
}

func (q *columnarQuerier) Close() error {
	if q.closed {
		return errors.New("columnar querier already closed")
	}

	errs := tsdb_errors.NewMulti(
	// TODO: close parquet file? Or readers?
	)
	q.parquetFile.Close()

	q.closed = true
	return errs.Err()
}

func buildSchemaForLabels(lbls []string, chunks bool) *parquet.Schema {
	// TODO: use common util
	node := parquet.Group{
		"x_series_id": parquet.Encoded(parquet.Int(64), &parquet.RLEDictionary),
	}
	for _, label := range lbls {
		// The metric name is not stored in the parquet file, so we don't need to include it in the schema.
		if label == labels.MetricName {
			continue
		}
		node["l_"+label] = parquet.String()
	}
	if chunks {
		node["x_chunk"] = parquet.Leaf(parquet.ByteArrayType)
		node["x_chunk_max_time"] = parquet.Encoded(parquet.Int(64), &parquet.DeltaBinaryPacked)
		node["x_chunk_min_time"] = parquet.Encoded(parquet.Int(64), &parquet.DeltaBinaryPacked)
	}
	return parquet.NewSchema("metric_family", node)
}

func matches(row parquet.Row, ms []*labels.Matcher, schema *parquet.Schema) bool {
	// TODO: very inefficient
	for _, val := range row {
		colName := schema.Columns()[val.Column()][0]
		if !strings.HasPrefix(colName, "l_") {
			continue
		}

		lname := colName[2:]
		// TODO: if we have dict encoding, is there a way to make this comparison quicker?

		lvalue := string(val.ByteArray())
		for _, m := range ms {
			if m.Name == lname && m.Value != lvalue {
				return false
			}
		}
	}
	return true
}

func (q *columnarQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.SeriesSet {
	metricFamily, err := q.getMetricFamily(ms)
	if err != nil {
		return storage.ErrSeriesSet(fmt.Errorf("we only accept matchers that have EQ matcher on __name__: %w", err))
	}

	f, err := os.Open(filepath.Join(q.dir, "data", fmt.Sprintf("%s.parquet", metricFamily)))

	if err != nil {
		panic(err)
	}

	q.parquetFile = f

	// Get the size of the parquet file.
	fstat, err := f.Stat()
	if err != nil {
		f.Close()
		panic(err)
	}

	pFile, err := parquet.OpenFile(f, fstat.Size())
	if err != nil {
		f.Close()
		panic(err)
	}

	root := pFile.Root()

	seriesIds, _ := loadSeriesIds(root)

	// Make a map of label name -> matchers
	labelMatchers := map[string][]*labels.Matcher{}
	for _, m := range ms {
		// Ignore the name matcher as we already have the metric family.
		if m.Name == labels.MetricName {
			continue
		}
		labelMatchers[m.Name] = append(labelMatchers[m.Name], m)
	}

	// excludeMask is true for rows that should NOT be included.
	var excludeMask []bool
	// Series labels is seriesId -> value of included labels
	var seriesLabels map[int64][]string = make(map[int64][]string)

	matchedLabels := []string{}
	for labelName, matchers := range labelMatchers {
		if excludeMask == nil {
			excludeMask = make([]bool, len(seriesIds))
		}

		// fmt.Printf("Filtering label %s\n", labelName)
		include := false
		if slices.Contains(q.includeLabels, labelName) {
			include = true
			matchedLabels = append(matchedLabels, labelName)
		}
		var labelValues []string
		excludeMask, labelValues, err = filterLabel(root, labelName, include, excludeMask, matchers...)
		if err != nil {
			panic(err)
		}

		updateSeriesLabels(seriesLabels, seriesIds, excludeMask, labelName, labelValues)
	}

	for _, l := range q.includeLabels {
		if slices.Contains(matchedLabels, l) {
			continue
		}
		if l == labels.MetricName {
			continue
		}

		labelValues, err := loadLabelValues(root, l)
		if err != nil {
			panic(err)
		}
		updateSeriesLabels(seriesLabels, seriesIds, excludeMask, l, labelValues)
	}


	return &columnarSeriesSet{
		metricName: metricFamily,
		seriesLabels: seriesLabels,
		chunkIterator: chunkColumnIterator{
			pf: pFile,
		},
		schema:  nil,
		mint:    q.mint,
		maxt:    q.maxt,
		builder: labels.NewScratchBuilder(1),
		curr:   rowsSeries{
            labels: labels.FromStrings(labels.MetricName, metricFamily),
		},
		seriesIds: seriesIds,
		excludeMask:      excludeMask,
	}
}

func updateSeriesLabels(seriesLabels map[int64][]string, seriesIds []int64, excludeMask []bool, labelName string, labelValues []string) {
	currentSeriesId := int64(0)
	for i, seriesId := range seriesIds {
		if currentSeriesId == seriesId {
			// Skip if we're in the same series.
			continue
		}
		currentSeriesId = seriesIds[i]
		if len(excludeMask) == 0 || !excludeMask[i] {
			seriesLabels[seriesIds[i]] = append(seriesLabels[seriesIds[i]], labelName, labelValues[i])
		}
	}
}

func loadSeriesIds(root *parquet.Column) ([]int64, error) {
	cols := root.Columns()
	var col *parquet.Column
	for _, c := range cols {
		if c.Name() == "x_series_id" {
			col = c
			break
		}
	}
	if col == nil {
		panic("x_series_id not found")
	}
	if !col.Leaf() {
		panic("x_series_id is not a leaf")
	}
	pages := col.Pages()
	seriesIds := []int64{}
	for {
		page, err := pages.ReadPage()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			panic(err)
		}

		valReader := page.Values()
		// fmt.Printf("Page has %d values, size %d\n", page.NumValues(), page.Size())

		// Read parquet.Value:
		nextSeriesIds := make([]parquet.Value, page.NumValues())
		_, err = valReader.ReadValues(nextSeriesIds)
		for _, v := range nextSeriesIds {
			seriesIds = append(seriesIds, v.Int64())
		}

		if err != nil && !errors.Is(err, io.EOF) {
			panic(err)
		}
	}
	return seriesIds, nil
}

func loadLabelValues(root *parquet.Column, labelName string) ([]string, error) {
	cols := root.Columns()
	var col *parquet.Column
	for _, c := range cols {
		if c.Name() == "l_"+labelName {
			col = c
			break
		}
	}
	if col == nil {
		panic("label not found")
	}
	if !col.Leaf() {
		panic("label is not a leaf")
	}
	pages := col.Pages()
	labelValues := []string{}
	for {
		page, err := pages.ReadPage()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			panic(err)
		}

		valReader := page.Values()
		// fmt.Printf("Page has %d values, size %d\n", page.NumValues(), page.Size())

		// Read parquet.Value:
		nextLabelValues := make([]parquet.Value, page.NumValues())
		_, err = valReader.ReadValues(nextLabelValues)
		for _, v := range nextLabelValues {
			labelValues = append(labelValues, v.String())
		}

		if err != nil && !errors.Is(err, io.EOF) {
			panic(err)
		}
	}
	return labelValues, nil
}

// filterLabel filters a label column based on matchers on the label.
// The input mask is updated and returned for rows that match the label.
// If include is true we also return the labels themselves.
func filterLabel(root *parquet.Column, labelName string, include bool, excludeMask []bool, matchers ...*labels.Matcher) ([]bool, []string, error) {
	cols := root.Columns()
	var col *parquet.Column
	for _, c := range cols {
		if c.Name() == "l_"+labelName {
			col = c
			break
		}
	}
	if col == nil {
		panic("label not found")
	}
	if !col.Leaf() {
		panic("label is not a leaf")
	}
	pages := col.Pages()
	matchingSymbols := []int32{}
	rowOffset := 0
	var labelValues []string
	for {
		page, err := pages.ReadPage()

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			panic(err)
		}

		symbols := page.Dictionary()
		matchingSymbols := matchingSymbols[:0]
		for i:=0; i<symbols.Len(); i++ {
			labelValue := symbols.Index(int32(i)).String()
			// fmt.Printf("Label value: %s for idx %d\n", labelValue, i)
			if matchLabel(labelValue, matchers...) {
				matchingSymbols = append(matchingSymbols, int32(i))
			}
		}

		if len(matchingSymbols) == 0 {
			// No symbols matched the label selector, mark all rows for exclude.
			for i := int64(0); i < page.NumValues(); i++ {
				excludeMask[rowOffset] = true
				rowOffset++
			}
			continue
		}

		data := page.Data()
		syms := data.Int32()
		for _, sym := range syms {
			if include {
				labelValues = append(labelValues, symbols.Index(sym).String())
			}
			// If row is not already excluded, only then check if it matches the label selector.
			if !excludeMask[rowOffset] {
				if !slices.Contains(matchingSymbols, sym) {
					// Row does not match the label selector, mark it for exclude.
					excludeMask[rowOffset] = true
				}
			}
			rowOffset++
		}

	}

	// fmt.Printf("Row excludeMask: %v\n", excludeMask)

	return excludeMask, labelValues, nil
}

func matchLabel(labelValue string, matchers ...*labels.Matcher) bool {
	for _, m := range matchers {
		switch m.Type {
		case labels.MatchEqual:
			if m.Value != labelValue {
				return false
			}
		case labels.MatchNotEqual:
			if m.Value == labelValue {
				return false
			}
		default:
			panic("only MatchEqual and MatchNotEqual are supported")
		}
	}
	return true
}

func chunkRangeOverlaps(chunkmint, chunkmaxt, mint, maxt int64) bool {
	return !(chunkmaxt < mint || chunkmint > maxt)
}

func (*columnarQuerier) getMetricFamily(ms []*labels.Matcher) (string, error) {
	var metricFamily string
	for _, m := range ms {
		if m.Type == labels.MatchEqual && m.Name == labels.MetricName {
			metricFamily = m.Value
			break
		}
	}
	if metricFamily == "" {
		return "", errors.New("no metric name provided")
	}
	return metricFamily, nil
}

type columnarSeriesSet struct {
	metricName string
	seriesLabels map[int64][]string

    chunkIterator  chunkColumnIterator

	schema      *parquet.Schema
	//columnIndex map[string]int

	mint, maxt int64

	curr   rowsSeries

	builder labels.ScratchBuilder
	err     error

	seriesIds   []int64
	excludeMask []bool
	seriesPos   int64
}

func (b *columnarSeriesSet) At() storage.Series {
	return &rowsSeries{
		labels: b.curr.labels.Copy(),
		metas: b.curr.metas,
	}
}

type rowsSeries struct {
	labels      labels.Labels
	metas       []chunks.Meta
}

type consecutiveChunkIterators struct {
	chunkenc.Iterator
	left []chunkenc.Iterator
}

func newConsecutiveChunkIterators(its []chunkenc.Iterator) *consecutiveChunkIterators {
	return &consecutiveChunkIterators{
		Iterator: its[0],
		left:     its[1:],
	}
}

func (c *consecutiveChunkIterators) Next() chunkenc.ValueType {
	rv := c.Iterator.Next()
	if rv != chunkenc.ValNone {
		return rv
	}

	if len(c.left) == 0 {
		return rv
	}

	c.Iterator = c.left[0]
	c.left = c.left[1:]
	return c.Next()
}

// Iterator implements storage.Series.
func (r *rowsSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {

	//pool := chunkenc.NewPool()
	// TODO: no closers
	its := make([]chunkenc.Iterator, 0, len(r.metas))
	// metas := make([]chunks.Meta, len(r.rows))
	for _, meta := range r.metas {
		its = append(its, meta.Chunk.Iterator(nil))
	}

	return newConsecutiveChunkIterators(its)
}

// Labels implements storage.Series.
func (r *rowsSeries) Labels() labels.Labels {
	return r.labels
}

func (b *columnarSeriesSet) Next() bool {
	if int(b.seriesPos) >= len(b.seriesIds) {
		return false
	}

	// Skip series that don't match the mask.
	if len(b.excludeMask) > 0 {
		for int(b.seriesPos) < len(b.seriesIds) && b.excludeMask[b.seriesPos] {
			b.seriesPos++
		}
		// Not found.
		if int(b.seriesPos) >= len(b.seriesIds) {
			return false
		}
		b.chunkIterator.SeekToRow(b.seriesPos)
	}

	b.curr.metas = []chunks.Meta{}
	nextSeriesId := b.seriesIds[b.seriesPos]
	for int(b.seriesPos) < len(b.seriesIds) && nextSeriesId == b.seriesIds[b.seriesPos] {
		chk, err := b.chunkIterator.Next()
		if err != nil {
			panic(err)
		}

		b.curr.metas = append(b.curr.metas, chunks.Meta{
			Chunk: chk,
		})
		b.seriesPos++
	}
	
	b.builder.Reset()
	b.builder.Add(labels.MetricName, b.metricName)
	for i:=0;i<len(b.seriesLabels[nextSeriesId]);i+=2 {
		b.builder.Add(b.seriesLabels[nextSeriesId][i], b.seriesLabels[nextSeriesId][i+1])
	}
	b.curr.labels = b.builder.Labels()

	return true
}

func (b *columnarSeriesSet) Err() error {
	if b.err != nil {
		return b.err
	}
	return nil // TODO?
}

func (b *columnarSeriesSet) Warnings() annotations.Annotations { return nil }


type chunkColumnIterator struct {
	pf             *parquet.File
	chunkPages     parquet.Pages
	chunkBuffer    []byte
	chunkBufferPos int
	rowId          int
}

// Next return the next chunk or nil if there are no more chunks.
func (c *chunkColumnIterator) Next() (chunkenc.Chunk, error) {
	// Sometimes there's an empty array at the end of a page.
	if c.chunkBufferPos < len(c.chunkBuffer)-4 {
		return c.currentChunk()
	}

	err := c.ensurePages()
	if err != nil {
		return nil, err
	}

	// We need to read the next page.
	page, err := c.chunkPages.ReadPage()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}

	valReader := page.Values()
	// fmt.Printf("Page has %d values, size %d\n", page.NumValues(), page.Size())
	if c.chunkBuffer == nil || int64(cap(c.chunkBuffer)) < page.Size() {
		c.chunkBuffer = make([]byte, page.Size())
	}
	c.chunkBuffer = c.chunkBuffer[:page.Size()]
	binaryReader, ok := valReader.(parquet.ByteArrayReader)
	if !ok {
		panic("not a ByteArrayReader")
	}
	_, err = binaryReader.ReadByteArrays(c.chunkBuffer)
	c.chunkBufferPos = 0
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return c.currentChunk()
}

func (c *chunkColumnIterator) ensurePages() error {
	// Init the pages if not already done.
	if c.chunkPages != nil {
		return nil
	}
	cols := c.pf.Root().Columns()
	var col *parquet.Column
	for _, c := range cols {
		if c.Name() == "x_chunk" {
			col = c
			break
		}
	}
	if col == nil {
		return errors.New("x_chunk not found")
	}
	c.chunkPages = col.Pages()
	return nil
}

func (c *chunkColumnIterator) currentChunk() (chunkenc.Chunk, error) {
	c.rowId++
	// We have a chunk in the buffer, return it.
	chunkLen := int(c.chunkBuffer[c.chunkBufferPos]) | int(c.chunkBuffer[c.chunkBufferPos+1])<<8 | int(c.chunkBuffer[c.chunkBufferPos+2])<<16 | int(c.chunkBuffer[c.chunkBufferPos+3])<<24
	chk, err := chunkenc.FromData(chunkenc.EncXOR, c.chunkBuffer[c.chunkBufferPos+4:c.chunkBufferPos+4+chunkLen])
	if err != nil {
		return nil, err
	}
	c.chunkBufferPos += 4 + chunkLen
	return chk, nil
}

// Seek to row.
func (c *chunkColumnIterator) SeekToRow(row int64) error {
	if err := c.ensurePages(); err != nil {
		return err
	}
	return c.chunkPages.SeekToRow(row)
}