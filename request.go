package arsyncfs


type ReadInfo struct {
	RemoteNode *RemoteNode
	Offset int64
	Size   int
}
