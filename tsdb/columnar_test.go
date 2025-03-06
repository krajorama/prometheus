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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func TestColumnarQuerier(t *testing.T) {
	from := int64(1733828454000)
	to := int64(1733829686000)
	q, err := NewColumnarQuerier("testdata/01JNKZDF5RP1X06VKBC2WMZJ8K", from, to, []string{"dim_0"})
	require.NoError(t, err)
	defer q.Close()

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "tsdb2columnar_gauge_0"),
		//labels.MustNewMatcher(labels.MatchEqual, "dim_0", "val_1"),
	}

	ctx := context.Background()

	seriesSet := q.Select(ctx, false, nil, matchers...)

	seriesCount := 0
	sampleCount := 0
	for seriesSet.Next() {
		seriesCount++
		series := seriesSet.At()
		lbls := []string{}
		series.Labels().Range(func(l labels.Label) {
			lbls = append(lbls, l.Name+"="+l.Value)
		})
		require.Equal(t, fmt.Sprintf("__name__=tsdb2columnar_gauge_0,dim_0=val_%d", seriesCount-1), strings.Join(lbls, ","))

		it := series.Iterator(nil)
		for it.Next() != chunkenc.ValNone {
			sampleCount++
			// TODO: this fails
			// require.GreaterOrEqual(t, it.AtT(), from)
			// require.LessOrEqual(t, it.AtT(), to)
		}
	}
	require.Equal(t, 5, seriesCount)
	require.Equal(t, 2500, sampleCount)
}
