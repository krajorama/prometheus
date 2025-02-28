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
	"errors"

	"github.com/prometheus/prometheus/storage"
)

// NewColumnarBlockQuerier (b BlockReader, mint, maxt int64).
func NewColumnarBlockQuerier(_ BlockReader, _, _ int64) (storage.Querier, error) {
	return nil, errors.New("not implemented")
}

// NewColumnarBlockChunkQuerier (b BlockReader, mint, maxt int64).
func NewColumnarBlockChunkQuerier(_ BlockReader, _, _ int64) (storage.ChunkQuerier, error) {
	return nil, errors.New("not implemented")
}
