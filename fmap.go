package ifs

import "sync"


type SyncStr struct {
	m *sync.RWMutex
	v string
}

func (s *SyncStr) Get() string {
	s.m.RLock()
	defer s.m.RUnlock()
	return s.v
}

func (s *SyncStr) Set(str string) {
	s.m.Lock()
	defer s.m.Unlock()
	s.v = str
}

func NewSyncStr(str string) *SyncStr {
	return &SyncStr{
		m: new(sync.RWMutex),
		v: str,
	}
}

type SyncUint struct {
	m *sync.RWMutex
	v uint
}

func (s *SyncUint) Get() uint {
	s.m.RLock()
	defer s.m.RUnlock()
	return s.v
}

func (s *SyncUint) Set(val uint) {
	s.m.Lock()
	defer s.m.Unlock()
	s.v = val
}

func NewSyncUint(val uint) *SyncUint {
	return &SyncUint{
		m: new(sync.RWMutex),
		v: val,
	}
}

