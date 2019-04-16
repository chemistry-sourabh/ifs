/*
 * Copyright 2019 Sourabh Bollapragada
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package structures

import (
	"github.com/OneOfOne/xxhash"
	"sync"
)

type MutexMap struct {
	m     []sync.Mutex
	count uint64
}

func NewMutexMap(bucketSize uint64) MutexMap {
	return MutexMap{
		m:     make([]sync.Mutex, bucketSize),
		count: bucketSize,
	}
}

func (mm *MutexMap) hash(key string) uint64 {
	h := xxhash.New64()
	_, _ = h.WriteString(key)
	return h.Sum64() % mm.count
}

func (mm *MutexMap) Lock(key string) {
	i := mm.hash(key)
	mm.m[i].Lock()
}

func (mm *MutexMap) Unlock(key string) {
	i := mm.hash(key)
	mm.m[i].Unlock()
}

