package arsyncfs

import (
	"encoding/base64"
	"log"
)

// Should Contain methods for all attributes like FileInfo
type BaseResponse interface {
	Id() uint64
	Op() uint8
	RemoteNode() *RemoteNode
}

type StatResponse struct {
	RequestId uint64      `json:"request_id"`
	RNode     *RemoteNode `json:"r_node"`
	Stat      *Stat       `json:"stat"`
}

func (sr *StatResponse) Id() uint64 {
	return sr.RequestId
}

func (sr *StatResponse) Op() uint8 {
	return AttrOp
}

func (sr *StatResponse) RemoteNode() *RemoteNode {
	return sr.RNode
}

type ReadDirResponse struct {
	RequestId uint64 `json:"request_id"`
	RNode     *RemoteNode
	Stats     []*Stat
}

func (rdr *ReadDirResponse) Id() uint64 {
	return rdr.RequestId
}

func (rdr *ReadDirResponse) Op() uint8 {
	return ReadDirOp
}

func (rdr *ReadDirResponse) RemoteNode() *RemoteNode {
	return rdr.RNode
}

type FileDataResponse struct {
	RequestId uint64 `json:"request_id"`
	RNode     *RemoteNode
	Data      string
}

func (fdr *FileDataResponse) Id() uint64 {
	return fdr.RequestId
}

func (fdr *FileDataResponse) Op() uint8 {
	return FetchFileOp
}

func (fdr *FileDataResponse) RemoteNode() *RemoteNode {
	return fdr.RNode
}

func (fdr *FileDataResponse) ExtractData() []byte {
	byteData, err := base64.StdEncoding.DecodeString(fdr.Data)

	if err != nil {
		log.Fatal(err)
	}

	return byteData
}
