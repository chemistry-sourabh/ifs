package arsyncfs

import "os"

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

type WriteResult struct {
	Size int
}
