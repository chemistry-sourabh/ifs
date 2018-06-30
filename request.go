package ifs

import (
	"bazil.org/fuse"
	"os"
)

type ReadDirInfo struct {
	Path           string
	FileDescriptor uint64
}

type ReadInfo struct {
	Path           string
	FileDescriptor uint64
	Offset         int64
	Size           int
}

type WriteInfo struct {
	Path           string
	FileDescriptor uint64
	Offset         int64
	Data           []byte
}

type AttrInfo struct {
	Path  string
	Valid fuse.SetattrValid
	Size  uint64
	Mode  os.FileMode
	ATime int64
	MTime int64
}

type CreateInfo struct {
	BaseDir        string
	Name           string
	IsDir          bool
	FileDescriptor uint64
}

type RenameInfo struct {
	Path     string
	DestPath string
}

type OpenInfo struct {
	Path           string
	FileDescriptor uint64
	Flags          int
	//Perm           int
}

type CloseInfo struct {
	Path           string
	FileDescriptor uint64
}

type FlushInfo struct {
	Path           string
	FileDescriptor uint64
}

type FetchInfo struct {
	RemotePath     *RemotePath
	FileDescriptor uint64
	Flags          int
}

type AttrUpdateInfo struct {
	RemotePath *RemotePath
	Size       int64
	Mode       os.FileMode
	ModTime    int64
}
