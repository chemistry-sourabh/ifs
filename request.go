package arsyncfs


type ReadInfo struct {
	RemotePath *RemotePath
	Offset int64
	Size   int
}
