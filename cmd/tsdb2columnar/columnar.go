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

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/parquet-go/parquet-go"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/columnar"
)

func compactToColumnarBlock(blockPath string, logger *slog.Logger) (string, error) {
	block, err := tsdb.OpenBlock(logger, blockPath, nil, nil)
	if err != nil {
		return "", fmt.Errorf("failed to open TSDB block: %w", err)
	}
	defer block.Close()

	destDir, err := os.MkdirTemp("", "")
	if err != nil {
		return "", fmt.Errorf("failed to create destination dir: %w", err)
	}

	compactor := &tsdb.ColumnarCompactor{}
	ids, err := compactor.Write(destDir, block, block.MinTime(), block.MaxTime(), nil)
	if err != nil {
		return "", fmt.Errorf("failed to compact to columnar block: %w", err)
	}

	if len(ids) != 1 {
		return "", fmt.Errorf("failed to compact to columnar block: expected 1 id but got %v", len(ids))
	}
	blockName := ids[0].String()
	columnarBlockPath := filepath.Join(filepath.Dir(blockPath), blockName+"_columnar")
	err = os.Rename(filepath.Join(destDir, blockName), columnarBlockPath)
	if err != nil {
		return "", fmt.Errorf("failed to move columnar block: %w", err)
	}

	logger.Info("Successfully compacted block to columnar", "original", blockPath, "columnar", columnarBlockPath)

	return columnarBlockPath, nil
}

type TimeSeriesRow struct {
	Lbls     []Label
	Chunk    []byte
	MinTime  int64
	MaxTime  int64
	SeriesID int64
}

type Label struct {
	Key   string
	Value string
}

func convertToColumnarBlock(blockPath string, logger *slog.Logger) error {
	columnarBlockPath := blockPath + "_columnar"
	dataDir := filepath.Join(columnarBlockPath, "data")

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("failed to create columnar block directory: %w", err)
	}

	bm, _, err := tsdb.ReadMetaFile(blockPath)
	if err != nil {
		return fmt.Errorf("failed to read meta file: %w", err)
	}
	if bm.Compaction.IsParquet() {
		return errors.New("block is already in columnar format")
	}
	bm.Compaction.SetParquet()

	_, err = tsdb.WriteMetaFile(logger, columnarBlockPath, bm)
	if err != nil {
		return fmt.Errorf("failed to write meta file: %w", err)
	}

	block, err := tsdb.OpenBlock(logger, blockPath, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to open TSDB block: %w", err)
	}
	defer block.Close()

	indexr, err := block.Index()
	if err != nil {
		return fmt.Errorf("failed to open block index: %w", err)
	}
	defer indexr.Close()

	chunkr, err := block.Chunks()
	if err != nil {
		return fmt.Errorf("failed to open block chunks: %w", err)
	}
	defer chunkr.Close()

	metricFamilies, err := groupSeriesByMetricFamily(indexr, chunkr, logger)
	if err != nil {
		return fmt.Errorf("failed to group series by metric family: %w", err)
	}

	newIndex := columnar.NewIndex()

	fileIndex := int32(1)

	for metricName, series := range metricFamilies {
		mm, err := writeParquetFile(metricName, series, dataDir, logger, fileIndex)
		if err != nil {
			return fmt.Errorf("failed to write Parquet file for metric %s: %w", metricName, err)
		}
		newIndex.Metrics[metricName] = mm
		fileIndex++
	}

	if err := columnar.WriteIndex(newIndex, columnarBlockPath); err != nil {
		return fmt.Errorf("failed to write index: %w", err)
	}

	logger.Info("Successfully converted block to columnar format",
		"original", blockPath,
		"columnar", columnarBlockPath)

	return nil
}

func groupSeriesByMetricFamily(
	indexr tsdb.IndexReader,
	chunkr tsdb.ChunkReader,
	_ *slog.Logger,
) (map[string][]TimeSeriesRow, error) {
	metricFamilies := make(map[string][]TimeSeriesRow)

	values, err := indexr.LabelValues(context.Background(), "__name__")
	if err != nil {
		return nil, fmt.Errorf("failed to get metric names: %w", err)
	}

	for _, metricName := range values {
		postings, err := indexr.Postings(context.Background(), "__name__", metricName)
		if err != nil {
			return nil, fmt.Errorf("failed to get postings for metric %s: %w", metricName, err)
		}

		seriesID := int64(1)

		for postings.Next() {
			seriesRef := postings.At()
			builder := labels.NewScratchBuilder(0)
			chks := []chunks.Meta{}
			err := indexr.Series(seriesRef, &builder, &chks)
			if err != nil {
				return nil, fmt.Errorf("failed to get chunk metas and labels from series")
			}

			labelSets := make([]Label, 0, builder.Labels().Len())
			builder.Labels().Range(func(lbl labels.Label) {
				labelSets = append(labelSets, Label{
					Key:   lbl.Name,
					Value: lbl.Value,
				})
			})

			for _, chk := range chks {
				c, iterable, err := chunkr.ChunkOrIterable(chk)
				if err != nil {
					return nil, fmt.Errorf("error reading chunk")
				}
				if iterable != nil {
					return nil, fmt.Errorf("ChunkOrIterable should not return an iterable when reading a block")
				}

				row := TimeSeriesRow{
					Lbls:     labelSets,
					Chunk:    c.Bytes(),
					MinTime:  chk.MinTime,
					MaxTime:  chk.MaxTime,
					SeriesID: seriesID,
				}

				metricFamilies[metricName] = append(metricFamilies[metricName], row)
			}

			seriesID++
		}

		if postings.Err() != nil {
			return nil, fmt.Errorf("error iterating postings: %w", postings.Err())
		}
	}

	return metricFamilies, nil
}

func writeParquetFile(
	metricName string,
	series []TimeSeriesRow,
	dataDir string,
	logger *slog.Logger,
	fileIndex int32,
) (columnar.MetricMeta, error) {
	metricMeta := columnar.MetricMeta{
		ParquetFile: metricName + ".parquet",
		LabelNames:  uniqueLabelKeys(series),
		FileIndex:   fileIndex,
	}

	schema := buildDynamicSchema(metricMeta.LabelNames)

	var parquetRows []parquet.Row
	parquetRows, metricMeta.MinT, metricMeta.MaxT = convertToParquetValues(series, schema)

	fileName := filepath.Join(dataDir, metricMeta.ParquetFile)
	f, err := os.Create(fileName)
	if err != nil {
		return metricMeta, fmt.Errorf("failed to create Parquet file: %w", err)
	}
	defer f.Close()

	writer := parquet.NewGenericWriter[any](f, schema)
	_, err = writer.WriteRows(parquetRows)
	if err != nil {
		return metricMeta, fmt.Errorf("failed to write rows to Parquet file: %w", err)
	}

	if err := writer.Close(); err != nil {
		return metricMeta, fmt.Errorf("failed to close Parquet writer: %w", err)
	}

	logger.Info("Created Parquet file", "metric", metricName, "file", fileName, "series", len(series))
	return metricMeta, nil
}

func uniqueLabelKeys(rows []TimeSeriesRow) []string {
	uniqueLabels := make(map[string]struct{})
	for _, row := range rows {
		for _, label := range row.Lbls {
			uniqueLabels[label.Key] = struct{}{}
		}
	}

	labelKeys := make([]string, 0, len(uniqueLabels))
	for key := range uniqueLabels {
		labelKeys = append(labelKeys, key)
	}
	sort.Strings(labelKeys)

	return labelKeys
}

func buildDynamicSchema(labelKeys []string) *parquet.Schema {
	node := parquet.Group{
		"x_series_id": parquet.Encoded(parquet.Int(64), &parquet.RLEDictionary),

		"x_chunk":          parquet.Leaf(parquet.ByteArrayType),
		"x_chunk_max_time": parquet.Encoded(parquet.Int(64), &parquet.DeltaBinaryPacked),
		"x_chunk_min_time": parquet.Encoded(parquet.Int(64), &parquet.DeltaBinaryPacked), // TODO For fixed intervals parquet.RLE might be better, we should test that
	}
	for _, label := range labelKeys {
		// We don't need to store the metric name as a label, it's already in the file name.
		if label == labels.MetricName {
			continue
		}
		node["l_"+label] = parquet.Encoded(parquet.String(), &parquet.RLEDictionary)
	}

	return parquet.NewSchema("metric_family", node)
}

func convertToParquetValues(rows []TimeSeriesRow, schema *parquet.Schema) ([]parquet.Row, int64, int64) {
	var minT, maxT int64 = math.MaxInt64, math.MinInt64
	columnMap := make(map[string]int)
	for i, col := range schema.Columns() {
		columnMap[col[0]] = i
	}

	result := make([]parquet.Row, len(rows))

	for i, row := range rows {
		values := make([]parquet.Value, len(schema.Columns()))

		seriesIDIdx := columnMap["x_series_id"]
		values[seriesIDIdx] = parquet.Int64Value(row.SeriesID)

		chunkIdx := columnMap["x_chunk"]
		values[chunkIdx] = parquet.ByteArrayValue(row.Chunk)

		minTimeIdx := columnMap["x_chunk_min_time"]
		values[minTimeIdx] = parquet.Int64Value(row.MinTime)
		if row.MinTime < minT {
			minT = row.MinTime
		}

		maxTimeIdx := columnMap["x_chunk_max_time"]
		values[maxTimeIdx] = parquet.Int64Value(row.MaxTime)
		if row.MaxTime > maxT {
			maxT = row.MaxTime
		}

		labelMap := make(map[string]string)
		for _, label := range row.Lbls {
			labelMap[label.Key] = label.Value
		}

		for key, value := range labelMap {
			if idx, ok := columnMap["l_"+key]; ok {
				values[idx] = parquet.ByteArrayValue([]byte(value))
			}
		}

		result[i] = values
	}

	return result, minT, maxT
}
