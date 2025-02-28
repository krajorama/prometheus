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
	"errors"
	"log/slog"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type ColumnarCompactorOptions struct{}

func NewColumnarCompactorWithOptions(ctx context.Context, r prometheus.Registerer, l *slog.Logger, ranges []int64, pool chunkenc.Pool, opts ColumnarCompactorOptions) (*ColumnarCompactor, error) {
	return nil, errors.New("not implemented")
}

// ColumnarCompactor implements the Compactor interface.
type ColumnarCompactor struct{}

func (c *ColumnarCompactor) Plan(dir string) ([]string, error) {
	// TODO: invoke the LevelCompactor.Plan method or something common.
	return nil, errors.New("not implemented")
}

func (c *ColumnarCompactor) Compact(dest string, dirs []string, open []*Block) ([]ulid.ULID, error) {
	// TODO: implement compaction over columnar blocks.
	return nil, errors.New("not implemented")
}

func (c *ColumnarCompactor) Write(dest string, b BlockReader, mint, maxt int64, base *BlockMeta) ([]ulid.ULID, error) {
	// TODO: implement writing out a (head) block.
	return nil, errors.New("not implemented")
}
