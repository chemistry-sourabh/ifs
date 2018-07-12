// +build unit

package unit

import (
	"testing"
	"github.com/google/go-cmp/cmp"
	"ifs"
	"ifs/test"
)

const remotePath = "localhost:1121@/tmp/"

var remotePathObject ifs.RemotePath = ifs.RemotePath{
	Hostname: "localhost",
	Port:     1121,
	Path:     "/tmp/",
}

func TestRemotePath_String(t *testing.T) {
	got := remotePathObject.String()

	if got != remotePath {
		test.PrintTestError(t, "string converted RemoteRoot not matching", got, remotePath)
	}
}

func TestRemotePath_Convert(t *testing.T) {
	rp := ifs.RemotePath{}
	rp.Convert(remotePath)

	if !cmp.Equal(rp, remotePathObject) {
		test.PrintTestError(t, "Convert Result not matching", rp, remotePathObject)
	}
}

func TestRemotePath_Address(t *testing.T) {
	got := remotePathObject.Address()

	address := "localhost:1121"
	if got != address {
		test.PrintTestError(t, "addresses not matching", got, address)
	}
}
