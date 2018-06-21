package ifs

import (
	"bazil.org/fuse"
	"os"
)

type ReadDirInfo struct {
	RemotePath *RemotePath
	FileDescriptor uint64
}

type ReadInfo struct {
	RemotePath     *RemotePath
	FileDescriptor uint64
	Offset         int64
	Size           int
}

type WriteInfo struct {
	RemotePath     *RemotePath
	FileDescriptor uint64
	Offset         int64
	Data           []byte
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
	BaseDir        *RemotePath
	Name           string
	IsDir          bool
	FileDescriptor uint64
}

type RenameInfo struct {
	RemotePath *RemotePath
	DestPath   string
}

type OpenInfo struct {
	RemotePath     *RemotePath
	FileDescriptor uint64
	Flags          int
	//Perm           int
}

type CloseInfo struct {
	RemotePath     *RemotePath
	FileDescriptor uint64
}

type FlushInfo struct {
	RemotePath     *RemotePath
	FileDescriptor uint64
}
