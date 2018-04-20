package ifs

import (
	"os"
	"compress/zlib"
	"bytes"
	"log"
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
func (fc *FileChunk) Compress() {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	w.Write(fc.Chunk)
	w.Close()
	fc.Chunk = b.Bytes()
}

func (fc *FileChunk) Decompress() {
	var b bytes.Buffer
	b.Write(fc.Chunk)
	r, err := zlib.NewReader(&b)

	if err != nil {
		log.Fatal("Shit just happened", err)
	}

	var out bytes.Buffer
	out.ReadFrom(r)
	r.Close()
	fc.Chunk = out.Bytes()
}

type WriteResult struct {
	Size int
}

type Error struct {
	Err error
}