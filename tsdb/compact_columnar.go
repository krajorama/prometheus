// Copyright 2025 The Prometheus Authors
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
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"

	"github.com/oklog/ulid"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/columnar"
)

const columnarDataDir = "data"

type ColumnarCompactorOptions struct{}

// NewColumnarCompactorWithOptions (ctx context.Context, r prometheus.Registerer, l *slog.Logger, ranges []int64, pool chunkenc.Pool, opts ColumnarCompactorOptions).
func NewColumnarCompactorWithOptions(_ context.Context, _ prometheus.Registerer, l *slog.Logger, _ []int64, _ chunkenc.Pool, _ ColumnarCompactorOptions) (*ColumnarCompactor, error) {
	return &ColumnarCompactor{
		logger: l,
	}, nil
}

// ColumnarCompactor implements the Compactor interface.
type ColumnarCompactor struct {
	logger *slog.Logger
}

// Plan (dir string).
func (c *ColumnarCompactor) Plan(_ string) ([]string, error) {
	// TODO: invoke the LevelCompactor.Plan method or something common.
	return nil, errors.New("not implemented")
}

// Compact (dest string, dirs []string, open []*Block).
func (c *ColumnarCompactor) Compact(_ string, _ []string, _ []*Block) ([]ulid.ULID, error) {
	// TODO: implement compaction over columnar blocks.
	return nil, errors.New("not implemented")
}

type columnarSeriesData struct {
	Labels labels.Labels
	Chunks []chunks.Meta
}

// Write implements Compactor interface.
// The base block meta is nil when compacting a head block.
func (c *ColumnarCompactor) Write(dir string, b BlockReader, mint, maxt int64, base *BlockMeta) ([]ulid.ULID, error) {
	uid := ulid.MustNew(ulid.Now(), rand.Reader)

	// TODO(v): populate BlockMeta.Stats
	meta := &BlockMeta{
		ULID:    uid,
		MinTime: mint,
		MaxTime: maxt,
	}
	meta.Compaction.Level = 1
	meta.Compaction.Sources = []ulid.ULID{uid}

	// We're compacting into parquet block.
	meta.Compaction.SetParquet()

	if base != nil {
		meta.Compaction.Parents = []BlockDesc{
			{ULID: base.ULID, MinTime: base.MinTime, MaxTime: base.MaxTime},
		}
		if base.Compaction.FromOutOfOrder() {
			meta.Compaction.SetOutOfOrder()
		}
	}

	err := c.write(dir, meta, b)
	if err != nil {
		return nil, err
	}

	return []ulid.ULID{uid}, nil
}

func (c *ColumnarCompactor) write(dir string, meta *BlockMeta, b BlockReader) error {
	blockDir := filepath.Join(dir, meta.ULID.String())
	blockDataDir := filepath.Join(blockDir, columnarDataDir)
	if err := os.MkdirAll(blockDataDir, 0750); err != nil {
		return fmt.Errorf("create block data directory: %w", err)
	}

	indexr, err := b.Index()
	if err != nil {
		return fmt.Errorf("get index reader: %w", err)
	}
	defer indexr.Close()

	postings := indexr.PostingsForAllLabelValues(context.Background(), labels.MetricName)

	// Group s in b by their metric names.
	metricFamilies := make(map[string][]columnarSeriesData)
	builder := labels.NewScratchBuilder(0)
	for postings.Next() {
		ref := postings.At()
		chks := []chunks.Meta{}
		err := indexr.Series(ref, &builder, &chks)
		if err != nil {
			return fmt.Errorf("get chunk metas and labels from series ref %d: %w", ref, err)
		}

		lbs := builder.Labels()

		// Series must have metric name
		metricName := lbs.Get(labels.MetricName)
		if metricName == "" {
			return fmt.Errorf("didn't find metric name for ref %v: label set %+v", ref, lbs)
		}

		metricFamilies[metricName] = append(metricFamilies[metricName], columnarSeriesData{
			Labels: lbs.DropMetricName(), // always exclude metric name from the series data
			Chunks: chks,
		})
	}
	if postings.Err() != nil {
		return fmt.Errorf("read series set: %w", postings.Err())
	}

	ix := columnar.NewIndex()

	for metricName, series := range metricFamilies {
		if err := c.writeMetricFamily(blockDataDir, b, &ix, metricName, series); err != nil {
			return fmt.Errorf("write metric family %s: %w", metricName, err)
		}
	}

	if err := columnar.WriteIndex(ix, blockDir); err != nil {
		return fmt.Errorf("write index: %w", err)
	}

	if _, err = WriteMetaFile(c.logger, blockDir, meta); err != nil {
		return fmt.Errorf("write meta: %w", err)
	}

	return nil
}

func (c *ColumnarCompactor) writeMetricFamily(dir string, b BlockReader, ix *columnar.Index, metricName string, series []columnarSeriesData) error {
	columnarMeta := columnar.MetricMeta{
		ParquetFile: metricName + ".parquet", // ParquetFile is the only field used currently.
	}
	ix.Metrics[metricName] = columnarMeta

	fileName := filepath.Join(dir, columnarMeta.ParquetFile)
	pf, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("create parquet file %s: %w", fileName, err)
	}
	defer pf.Close()

	schema := buildColumnarSchemaFromSeries(metricName, series)

	cols := make(map[string]int, len(schema.Fields()))
	for p, f := range schema.Fields() {
		if !f.Leaf() {
			return fmt.Errorf("schema fields must be flat but found a non-leaf node at %s", f.Name())
		}
		cols[f.Name()] = p
	}

	pw := parquet.NewGenericWriter[any](pf, schema)
	defer pw.Close()

	chunkr, err := b.Chunks()
	if err != nil {
		return fmt.Errorf("get chunk reader: %w", err)
	}
	defer chunkr.Close()

	rows := make([]parquet.Row, 0)
	for sid, s := range series {
		for _, meta := range s.Chunks {
			row := make(parquet.Row, len(cols))

			// We expect to have one chunk per series meta.
			chunk, _, err := chunkr.ChunkOrIterable(meta)
			if err != nil {
				return fmt.Errorf("get chunk metas: %w", err)
			}

			for _, l := range s.Labels {
				p, ok := cols[columnNameForLabel(l.Name)]
				if !ok || p >= len(row)-4 { // 4 columns belong to the chunk data
					return fmt.Errorf("unexpected column index for label %s", l.Name)
				}
				row[p] = parquet.ByteArrayValue([]byte(l.Value))
			}

			row[cols["x_series_id"]] = parquet.Int64Value(int64(sid + 1)) // series_id start with 1
			row[cols["x_chunk"]] = parquet.ByteArrayValue(chunk.Bytes())
			row[cols["x_chunk_min_time"]] = parquet.Int64Value(meta.MinTime)
			row[cols["x_chunk_max_time"]] = parquet.Int64Value(meta.MaxTime)

			rows = append(rows, row)
		}
	}

	_, err = pw.WriteRows(rows)
	if err != nil {
		return fmt.Errorf("write parquet rows: %w", err)
	}
	return nil
}

func buildColumnarSchemaFromSeries(metricName string, series []columnarSeriesData) *parquet.Schema {
	labelNameSet := make(map[string]struct{})
	for _, s := range series {
		for _, l := range s.Labels {
			if l.Name == labels.MetricName {
				// don't add "__name__" into the columns
				continue
			}
			labelNameSet[l.Name] = struct{}{}
		}
	}
	labelNames := make([]string, 0, len(labelNameSet))
	for k := range labelNameSet {
		labelNames = append(labelNames, k)
	}
	sort.Strings(labelNames)

	node := make(parquet.Group, len(labelNames)+3)
	for _, label := range labelNames {
		node[columnNameForLabel(label)] = parquet.Encoded(parquet.String(), &parquet.RLEDictionary)
	}
	node["x_series_id"] = parquet.Encoded(parquet.Int(64), &parquet.RLEDictionary)
	node["x_chunk"] = parquet.Leaf(parquet.ByteArrayType)
	node["x_chunk_min_time"] = parquet.Encoded(parquet.Int(64), &parquet.DeltaBinaryPacked)
	node["x_chunk_max_time"] = parquet.Encoded(parquet.Int(64), &parquet.DeltaBinaryPacked)

	return parquet.NewSchema(metricName, node)
}

func columnNameForLabel(l string) string {
	return "l_" + l
}
