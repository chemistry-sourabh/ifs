/*
Copyright 2018 Sourabh Bollapragada

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package ifs

import (
	"sync"
	"hash/fnv"
)

type MutexMap struct {
	m []sync.Mutex
	count uint32
}

func NewMutexMap() *MutexMap {
	return &MutexMap{
		m: make([]sync.Mutex, 100),
		count: 100,
	}
}

func (mm *MutexMap) hash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % mm.count
}

func (mm *MutexMap) Lock(key string) {
	i := mm.hash(key)
	mm.m[i].Lock()
}

func (mm *MutexMap) Unlock(key string) {
	i := mm.hash(key)
	mm.m[i].Unlock()
}
