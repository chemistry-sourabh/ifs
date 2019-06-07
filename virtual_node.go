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
	"bazil.org/fuse/fs"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"os"
	"os/user"
	"strconv"
	"sync"
	"time"
)

type VirtualNode struct {
	Nodes *sync.Map
}

func (vn *VirtualNode) Attr(ctx context.Context, attr *fuse.Attr) error {

	zap.L().Debug("Attr FS Request",
		zap.Bool("vn", true),
		zap.String("op", "attr"),
	)

	curUser, _ := user.Current()
	uid, _ := strconv.ParseUint(curUser.Uid, 10, 64)

	curGroup, _ := user.LookupGroup("staff")
	gid, _ := strconv.ParseUint(curGroup.Gid, 10, 64)

	attr.Uid = uint32(uid)
	attr.Gid = uint32(gid)
	//attr.Size = uint64(10)
	attr.Mode = os.FileMode(os.ModeDir | 0755)
	attr.Valid = time.Duration(-1)

	zap.L().Debug("Attr Response",
		zap.Bool("vn", true),
		zap.String("op", "attr"),
	)

	return nil
}

func (vn *VirtualNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {

	zap.L().Debug("ReadDir FS Request",
		zap.Bool("vn", true),
		zap.String("op", "readdir"),
	)

	var children []fuse.Dirent

	vn.Nodes.Range(func(dirName, value interface{}) bool {
		child := fuse.Dirent{Type: fuse.DT_Dir, Name: dirName.(string)}
		children = append(children, child)
		return true
	})

	zap.L().Debug("ReadDir Response",
		zap.Bool("vn", true),
		zap.String("op", "readdir"),
		//zap.Strings("remote_roots", root.RemoteRoots),
	)

	return children, nil
}

func (vn *VirtualNode) Lookup(ctx context.Context, name string) (fs.Node, error) {

	zap.L().Debug("Lookup FS Request",
		zap.Bool("vn", true),
		zap.String("op", "lookup"),
		zap.String("name", name),
	)

	val, ok := vn.Nodes.Load(name)

	zap.L().Debug("Lookup Response",
		zap.Bool("vn", true),
		zap.String("op", "lookup"),
		zap.String("name", name),
		zap.Bool("ok", ok),
	)

	if ok {
		return val.(fs.Node), nil
	} else {
		return nil, fuse.ENOENT
	}
}
