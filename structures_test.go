// +build unit

package ifs_test

import (
	"testing"
	"github.com/google/go-cmp/cmp"
	"ifs"
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
		PrintTestError(t, "string converted RemoteRoot not matching", got, remotePath)
	}
}

func TestRemotePath_Convert(t *testing.T) {
	rp := ifs.RemotePath{}
	rp.Convert(remotePath)

	if !cmp.Equal(rp, remotePathObject) {
		PrintTestError(t, "Convert Result not matching", rp, remotePathObject)
	}
}

func TestRemotePath_Address(t *testing.T) {
	got := remotePathObject.Address()

	address := "localhost:1121"
	if got != address {
		PrintTestError(t, "addresses not matching", got, address)
	}
}
