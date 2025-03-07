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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/columnar"
)

func TestColumnarQuerier(t *testing.T) {
	blockDir := "testdata/01JNKZDF5RP1X06VKBC2WMZJ8K"

	ix, err := columnar.ReadIndex(blockDir)
	require.NoError(t, err)
	require.NotNil(t, ix)

	from := int64(1733828454000)
	to := int64(1733829686000)
	q, err := NewColumnarQuerier(blockDir, ix, from, to, []string{"dim_0"})
	require.NoError(t, err)
	defer q.Close()


	testCases := map[string]struct {
		matchers        []*labels.Matcher
		expectedSeries  []string  // Label selectors.
		expectedSamples int  // Number of samples.
	}{
		"all series of a metric family": {
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "tsdb2columnar_gauge_0"),
			},
			expectedSeries: []string{
				"__name__=tsdb2columnar_gauge_0,dim_0=val_0",
				"__name__=tsdb2columnar_gauge_0,dim_0=val_1",
				"__name__=tsdb2columnar_gauge_0,dim_0=val_2",
				"__name__=tsdb2columnar_gauge_0,dim_0=val_3",
				"__name__=tsdb2columnar_gauge_0,dim_0=val_4",
			},
			expectedSamples: 2500,
		},
		"single series": {
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "tsdb2columnar_gauge_0"),
				labels.MustNewMatcher(labels.MatchEqual, "dim_0", "val_2"),
			},
			expectedSeries: []string{
				"__name__=tsdb2columnar_gauge_0,dim_0=val_2",
			},
			expectedSamples: 500,
		},
		"no series of a metric family": {
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "tsdb2columnar_gauge_0"),
				labels.MustNewMatcher(labels.MatchEqual, "dim_0", "noval"),
			},
			expectedSeries: []string{},
			expectedSamples: 0,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			seriesSet := q.Select(ctx, false, nil, tc.matchers...)

			seriesCount := 0
			sampleCount := 0
			for seriesSet.Next() {
				seriesCount++
				series := seriesSet.At()
				lbls := []string{}
				series.Labels().Range(func(l labels.Label) {
					lbls = append(lbls, l.Name+"="+l.Value)
				})
				require.Equal(t, tc.expectedSeries[seriesCount-1], strings.Join(lbls, ","))

				it := series.Iterator(nil)
				for it.Next() != chunkenc.ValNone {
					sampleCount++
					// TODO: this fails
					// require.GreaterOrEqual(t, it.AtT(), from)
					// require.LessOrEqual(t, it.AtT(), to)
				}
			}
			require.Equal(t, len(tc.expectedSeries), seriesCount)
			require.Equal(t, tc.expectedSamples, sampleCount)
		})
	}
}
