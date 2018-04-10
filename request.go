package arsyncfs

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

type TruncInfo struct {
	RemotePath *RemotePath
	Size       uint64
}
