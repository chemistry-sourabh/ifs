package ifs

import (
	"fmt"
	"bazil.org/fuse"
	"os"
)

type ReadInfo struct {
	RemotePath *RemotePath
	Offset     int64
	Size       int
}

func (ri *ReadInfo) String() string {
	return fmt.Sprintf("RemotePath = %s Offset = %d Size = %d", ri.RemotePath, ri.Offset, ri.Size)
}

type WriteInfo struct {
	RemotePath *RemotePath
	Offset     int64
	Data       []byte
}

type AttrInfo struct {
	RemotePath *RemotePath
	Valid      fuse.SetattrValid
	Size       uint64
	Mode	   os.FileMode
	ATime	   int64
	MTime	   int64
}

type CreateInfo struct {
	BaseDir *RemotePath
	Name    string
	IsDir   bool
}
