package ifs

import "fmt"

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



type TruncInfo struct {
	RemotePath *RemotePath
	Size       uint64
}

type CreateInfo struct {
	BaseDir *RemotePath
	Name    string
	IsDir   bool
}
