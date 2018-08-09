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
