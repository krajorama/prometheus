package main

import (
	"context"
	"math"
	"testing"

	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/columnar"
)

const (
	numSeries   = 1
	dimensions  = 1
	cardinality = 100_000
)


func BenchmarkColumnarQuerier(b *testing.B) {
	tmpDir := b.TempDir()

	logger := promslog.NewNopLogger()

	blockDir, err := createTSDBBlock(numSeries, tmpDir, dimensions, cardinality, 0, logger)
	require.NoError(b, err)

	columnarBlockDir, err := convertToColumnarBlock(blockDir, logger)
	require.NoError(b, err)

	b.Run("Block", func(b *testing.B) {
		block, err := tsdb.OpenBlock(logger, blockDir, nil, nil)
		require.NoError(b, err)
		defer func() {
			require.NoError(b, block.Close())
		}()

		benchmarkXXXSelect(b, (*queryableBlock)(block))
	})

	b.Run("ColumnarBlock", func(b *testing.B) {
		benchmarkXXXSelect(b, columnarQueryableBlockFromDir(columnarBlockDir))
	})
}

type queryableBlock tsdb.Block

func (pb *queryableBlock) Querier(mint, maxt int64) (storage.Querier, error) {
	return tsdb.NewBlockQuerier((*tsdb.Block)(pb), mint, maxt)
}

type columnarQueryableBlockFromDir string

func (dir columnarQueryableBlockFromDir) Querier(mint, maxt int64) (storage.Querier, error) {
	ix, err := columnar.ReadIndex(string(dir))
	if err != nil {
		return nil, err
	}

	return tsdb.NewColumnarQuerier(string(dir), ix, mint, maxt, nil)
}

func benchmarkXXXSelect(b *testing.B, queryable storage.Queryable) {
	matcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", "tsdb2columnar_gauge_0")
	q, err := queryable.Querier(0, math.MaxInt64)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()

	var series int
	for i := 0; i < b.N; i++ {
		ss := q.Select(context.Background(), false, nil, matcher)
		var it chunkenc.Iterator
		for ss.Next() {
			s := ss.At()
			// Call the function to get labels.
			s.Labels()
			it := s.Iterator(it)
			for it.Next() != chunkenc.ValNone {
				// Iteries over all samples.
			}
			if it.Err() != nil {
				b.Fatal(it.Err())
			}
			series++
		}
		if err := ss.Err(); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(series)/float64(b.N), "series/op")

	q.Close()
}
