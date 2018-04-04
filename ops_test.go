package arsyncfs

import (
	"testing"
	"arsyncfs/fs"
	"arsyncfs"
)

func TestAttr(t *testing.T) {
	req := Request{
		0,
		fs.RemoteNode{
			RemotePath: arsyncfs.RemotePath{
				Hostname: "localhost",
				Port:     1121,
				Path:     "/Users/sourabh/Downloads/2.diff",
			},
		},
	}

	Attr(req)
}
