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
	Err   error
}

// TODO Should move error to packet
type WriteResult struct {
	Size int
	Err error
}

//// Should Contain methods for all attributes like FileInfo
//type BaseResponse interface {
//	Id() uint64
//	Op() uint8
//	RemoteNode() *RemoteNode
//}
//
//type StatResponse struct {
//	RequestId uint64      `json:"request_id"`
//	RNode     *RemoteNode `json:"r_node"`
//	Stat      *Stat       `json:"stat"`
//}
//
//func (sr *StatResponse) Id() uint64 {
//	return sr.RequestId
//}
//
//func (sr *StatResponse) Op() uint8 {
//	return AttrRequest
//}
//
//func (sr *StatResponse) RemoteNode() *RemoteNode {
//	return sr.RNode
//}
//
//type ReadDirResponse struct {
//	RequestId uint64 `json:"request_id"`
//	RNode     *RemoteNode
//	Stats     []*Stat
//}
//
//func (rdr *ReadDirResponse) Id() uint64 {
//	return rdr.RequestId
//}
//
//func (rdr *ReadDirResponse) Op() uint8 {
//	return ReadDirRequest
//}
//
//func (rdr *ReadDirResponse) RemoteNode() *RemoteNode {
//	return rdr.RNode
//}
//
//type FileDataResponse struct {
//	RequestId uint64 `json:"request_id"`
//	RNode     *RemoteNode
//	Chunk      string
//}
//
//func (fdr *FileDataResponse) Id() uint64 {
//	return fdr.RequestId
//}
//
//func (fdr *FileDataResponse) Op() uint8 {
//	return FetchFileRequest
//}
//
//func (fdr *FileDataResponse) RemoteNode() *RemoteNode {
//	return fdr.RNode
//}
//
//func (fdr *FileDataResponse) ExtractData() []byte {
//	byteData, err := base64.StdEncoding.DecodeString(fdr.Chunk)
//
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	return byteData
//}
