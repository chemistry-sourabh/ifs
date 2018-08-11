// +build unit

/*
Copyright 2018 Sourabh Bollapragada

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package ifs_test

import (
	"testing"
	"github.com/google/go-cmp/cmp"
	"github.com/chemistry-sourabh/ifs"
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
