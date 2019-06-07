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

package structure

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"github.com/OneOfOne/xxhash"
	"go.uber.org/zap"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

// TODO Skip compression if file is too small
func (dm *DataMessage) Compress() {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	_, err := w.Write(dm.Data)
	if err != nil {
		zap.L().Fatal("Compression Failed",
			zap.Error(err),
		)
	}

	err = w.Close()
	if err != nil {
		zap.L().Warn("Couldnt Close Writer",
			zap.Error(err),
		)
	}

	dm.Data = b.Bytes()
}

func (dm *DataMessage) Decompress() {
	var b bytes.Buffer
	b.Write(dm.Data)
	r, err := zlib.NewReader(&b)

	if err != nil && err != io.EOF {
		zap.L().Fatal("Decompression Failed",
			zap.Error(err),
		)
	}

	var out bytes.Buffer
	_, err = out.ReadFrom(r)
	if err != nil {
		zap.L().Fatal("Decompression Failed",
			zap.Error(err),
		)
	}

	dm.Data = out.Bytes()
	err = r.Close()

	if err != nil {
		zap.L().Warn("Decompression Failed",
			zap.Error(err),
		)
	}
}

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

type RemotePath struct {
	Hostname string
	Port     uint16
	Path     string
}

func (rp *RemotePath) String() string {
	return fmt.Sprintf("%s:%d@%s", rp.Hostname, rp.Port, rp.Path)
}

func (rp *RemotePath) Load(str string) {
	parts := strings.Split(str, ":")
	rp.Hostname = parts[0]
	parts = strings.Split(parts[1], "@")
	p64, _ := strconv.ParseUint(parts[0], 10, 32)
	rp.Port = uint16(p64)
	rp.Path = parts[1]
}

func (rp *RemotePath) Address() string {
	return fmt.Sprintf("%s:%d", rp.Hostname, rp.Port)
}

type CacheFileHandle struct {
	FilePath *RemotePath
	Fp       *os.File
}

type FileInfo struct {
	Name  string
	Size  uint64
	Mode  uint32
	Mtime uint64
	IsDir bool
}
