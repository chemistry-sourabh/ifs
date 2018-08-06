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

package ifs

import (
	"bazil.org/fuse"
	"os"
)

type ReadDirInfo struct {
	Path           string
	FileDescriptor uint64
}

type ReadInfo struct {
	Path           string
	FileDescriptor uint64
	Offset         int64
	Size           int
}

type WriteInfo struct {
	Path           string
	FileDescriptor uint64
	Offset         int64
	Data           []byte
}

type AttrInfo struct {
	Path  string
	Valid fuse.SetattrValid
	Size  uint64
	Mode  os.FileMode
	ATime int64
	MTime int64
}

type CreateInfo struct {
	BaseDir        string
	Name           string
	IsDir          bool
	FileDescriptor uint64
}

type RenameInfo struct {
	Path     string
	DestPath string
}

type OpenInfo struct {
	Path           string
	FileDescriptor uint64
	Flags          fuse.OpenFlags
	//Perm           int
}

type CloseInfo struct {
	Path           string
	FileDescriptor uint64
}

type FlushInfo struct {
	Path           string
	FileDescriptor uint64
}

type FetchInfo struct {
	RemotePath     *RemotePath
	FileDescriptor uint64
	Flags          fuse.OpenFlags
}
