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
	"os"
				)

type Stat struct {
	Name    string
	Size    int64
	Mode    os.FileMode
	ModTime int64
	IsDir   bool
}

type DirInfo struct {
	Stats []*Stat
}

type FileChunk struct {
	Chunk []byte
	Size  int
}

// TODO Skip compression if file is too small
//func (fc *FileChunk) Compress() {
//	var b bytes.Buffer
//	w := zlib.NewWriter(&b)
//	defer w.Close()
//	w.Write(fc.Chunk)
//	fc.Chunk = b.Bytes()
//}
//
//func (fc *FileChunk) Decompress() {
//	var b bytes.Buffer
//	b.Write(fc.Chunk)
//	r, err := zlib.NewReader(&b)
//	defer r.Close()
//
//	if err != nil && err != io.EOF {
//		zap.L().Fatal("Decompression Failed",
//			zap.Error(err),
//		)
//	}
//
//	var out bytes.Buffer
//	out.ReadFrom(r)
//	fc.Chunk = out.Bytes()
//}

type WriteResult struct {
	Size int
	FileSize int64
}

type Error struct {
	Err error
}
