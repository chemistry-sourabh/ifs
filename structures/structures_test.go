// +build unit

/*
 * Copyright 2019 Sourabh Bollapragada
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package structures_test

import (
	"github.com/chemistry-sourabh/ifs/ifstest"
	"github.com/chemistry-sourabh/ifs/structures"
	"testing"
)

const remotePath = "localhost:1121@/tmp/"

var remotePathObject = structures.RemotePath{
	Hostname: "localhost",
	Port:     1121,
	Path:     "/tmp/",
}

func TestRemotePath_PrettyString(t *testing.T) {
	got := remotePathObject.PrettyString()

	ifstest.Compare(t, got, remotePath)
}

func TestRemotePath_Load(t *testing.T) {
	rp := structures.RemotePath{}
	rp.Load(remotePath)

	ifstest.Compare(t, rp, remotePathObject)
}

func TestRemotePath_Address(t *testing.T) {
	got := remotePathObject.Address()

	ifstest.Compare(t, got, "localhost:1121")
}
