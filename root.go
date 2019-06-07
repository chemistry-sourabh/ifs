///*
//Copyright 2018 Sourabh Bollapragada
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//*/
//
package ifs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/chemistry-sourabh/ifs/cache_manager"
	"github.com/chemistry-sourabh/ifs/structure"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type root struct {
	RemoteRoots *sync.Map
}

func NewRoot(remoteRoots []*RemoteRoot, cacheManager cache_manager.CacheManager) *root {
	root := &root{
		RemoteRoots: &sync.Map{},
	}

	root.RemoteRoots = generateRemoteRoots(remoteRoots, cacheManager)
	return root
}

// TODO All Errors should be resolved here
func (root *root) Root() (fs.Node, error) {
	return root, nil
}

func (root *root) Attr(ctx context.Context, attr *fuse.Attr) error {

	zap.L().Debug("Attr FS Request",
		zap.Bool("root", true),
		zap.String("op", "attr"),
	)

	// Check Error
	curUser, _ := user.Current()
	uid, _ := strconv.ParseUint(curUser.Uid, 10, 64)

	curGroup, _ := user.LookupGroup("staff")
	gid, _ := strconv.ParseUint(curGroup.Gid, 10, 64)

	attr.Uid = uint32(uid)
	attr.Gid = uint32(gid)
	//attr.Size = uint64(10)
	attr.Mode = os.FileMode(os.ModeDir | 0755)
	attr.Valid = time.Duration(-1)
	//attr.Mtime = s.ModTime

	zap.L().Debug("Attr Response",
		zap.Bool("root", true),
		zap.String("op", "attr"),
	)

	return nil
}

func (root *root) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {

	zap.L().Debug("ReadDir FS Request",
		zap.Bool("root", true),
		zap.String("op", "readdir"),
	)

	var children []fuse.Dirent

	root.RemoteRoots.Range(func(dirName, value interface{}) bool {
		child := fuse.Dirent{Type: fuse.DT_Dir, Name: dirName.(string)}
		children = append(children, child)
		return true
	})


	zap.L().Debug("ReadDir Response",
		zap.Bool("root", true),
		zap.String("op", "readdir"),
		//zap.Strings("remote_roots", root.RemoteRoots),
	)

	return children, nil
}

func (root *root) Lookup(ctx context.Context, name string) (fs.Node, error) {

	zap.L().Debug("Lookup FS Request",
		zap.Bool("root", true),
		zap.String("op", "lookup"),
		zap.String("name", name),
	)

	val, ok := root.RemoteRoots.Load(name)

	zap.L().Debug("Lookup Response",
		zap.Bool("root", true),
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

func generateVirtualNodes(paths []string, remotePaths []*structure.RemotePath, cacheManager cache_manager.CacheManager) *sync.Map {

	aggPaths := make(map[string][]string)
	aggRemotePaths := make(map[string][]*structure.RemotePath)
	virtualNodes := &sync.Map{}

	for i, p := range paths {

		l := strings.Split(strings.Trim(p, "/"), "/")

		if l[0] != "" {
			firstDir := l[0]
			aggPaths[firstDir] = append(aggPaths[firstDir], filepath.Join(l[1:]...))
			aggRemotePaths[firstDir] = append(aggRemotePaths[firstDir], remotePaths[i])
		}

	}

	for k, v := range aggPaths {

		if len(v) > 1 || (len(v) == 1 && v[0] != "") {
			virtualNodes.Store(k, &VirtualNode{
				Nodes: generateVirtualNodes(v, aggRemotePaths[k], cacheManager),
			})
		} else {
			nodes := &sync.Map{}
			virtualNodes.Store(k, &RemoteNode{
				IsDir:       true,
				RemotePath:  aggRemotePaths[k][0],
				RemoteNodes: nodes,
				CacheManager: cacheManager,
			})
		}
	}

	return virtualNodes
}

func generateRemoteRoot(paths []string, remotePaths []*structure.RemotePath, cacheManager cache_manager.CacheManager) *VirtualNode {

	return &VirtualNode{
		Nodes: generateVirtualNodes(paths, remotePaths, cacheManager),
	}
}

func generateRemoteRoots(remoteRoots []*RemoteRoot, cacheManager cache_manager.CacheManager) *sync.Map {

	virtualNodes := &sync.Map{}

	for _, remoteRoot := range remoteRoots {
		vn := generateRemoteRoot(remoteRoot.Paths, remoteRoot.RemotePaths(), cacheManager)
		virtualNodes.Store(remoteRoot.Hostname, vn)
	}

	return virtualNodes
}
