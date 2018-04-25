package ifs

import (
	"bazil.org/fuse"
	"os"
)

type ReadInfo struct {
	RemotePath *RemotePath
	Offset     int64
	Size       int
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
	Mode       os.FileMode
	ATime      int64
	MTime      int64
}

type CreateInfo struct {
	BaseDir *RemotePath
	Name    string
	IsDir   bool
}

type RenameInfo struct {
	RemotePath *RemotePath
	DestPath   string
}
