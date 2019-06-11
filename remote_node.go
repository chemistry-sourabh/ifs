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
	"context"
	"github.com/chemistry-sourabh/ifs/cache_manager"
	"github.com/chemistry-sourabh/ifs/structure"
	"go.uber.org/zap"
	"os"
	"os/user"
	"path"
	"strconv"
	"sync"
	"time"
)

type RemoteNode struct {
	RemotePath *structure.RemotePath

	IsDir    bool
	IsCached bool
	Size     uint64
	Mode     os.FileMode
	Mtime    time.Time
	Atime    time.Time
	// TODO Add Atime also

	// Children
	RemoteNodes *sync.Map

	CacheManager cache_manager.CacheManager
}

func (rn *RemoteNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	// Update fileHandler if invalid

	zap.L().Debug("Attr FS Request",
		zap.String("op", "attr"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
	)

	if !rn.IsCached {

		fi, err := rn.CacheManager.Attr(rn.RemotePath)

		if err != nil {

			zap.L().Warn("Attr Error Response",
				zap.String("op", "attr"),
				zap.String("address", rn.RemotePath.Address()),
				zap.String("path", rn.RemotePath.Path),
				zap.Error(err),
			)

			return err
		}

		zap.L().Debug("Attr Response From Agent",
			zap.String("op", "attr"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.String("mode", os.FileMode(fi.Mode).String()),
			zap.Uint64("size", fi.Size),
			zap.Time("mtime", time.Unix(0, int64(fi.Mtime))),
			zap.Time("atime", time.Unix(0, int64(fi.Atime))),
		)

		rn.Size = fi.Size
		rn.Mode = os.FileMode(fi.Mode)
		rn.Mtime = time.Unix(0, int64(fi.Mtime))
		rn.Atime = time.Unix(0, int64(fi.Atime))
		rn.IsCached = true

	}

	// Check Error
	curUser, _ := user.Current()
	uid, _ := strconv.ParseUint(curUser.Uid, 10, 64)

	curGroup, _ := user.LookupGroup("staff")
	gid, _ := strconv.ParseUint(curGroup.Gid, 10, 64)

	attr.Uid = uint32(uid)
	attr.Gid = uint32(gid)
	attr.Size = rn.Size
	attr.Mode = rn.Mode
	attr.Mtime = rn.Mtime
	attr.Atime = rn.Atime
	attr.Valid = time.Duration(-1)

	zap.L().Debug("Attr Response",
		zap.String("op", "attr"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("mode", rn.Mode.String()),
		zap.Uint64("size", rn.Size),
		zap.Time("mtime", rn.Mtime),
		zap.Time("atime", rn.Atime),
	)

	return nil
}

// TODO Should be Helper
func (rn *RemoteNode) generateChildRemoteNode(name string, isDir bool) *RemoteNode {

	return &RemoteNode{
		IsDir:    isDir,
		IsCached: false,
		RemotePath: &structure.RemotePath{
			Hostname: rn.RemotePath.Hostname,
			Port:     rn.RemotePath.Port,
			Path:     path.Join(rn.RemotePath.Path, name),
		},
		RemoteNodes:  &sync.Map{},
		CacheManager: rn.CacheManager,
	}
}

func (rn *RemoteNode) updateChildrenRemoteNodes() error {
	zap.L().Debug("ReadDir FS Request",
		zap.String("op", "ReadDir"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
	)

	fileInfos, err := rn.CacheManager.ReadDir(rn.RemotePath)

	if err != nil {
		zap.L().Warn("ReadDir Error Response",
			zap.String("op", "readdir"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.Error(err),
		)

		return err
	}

	fileNodes := &sync.Map{}
	//rn.RemoteNodes = make(map[string]*RemoteNode)

	zap.L().Debug("ReaddirAll Response from Agent",
		zap.String("op", "readdir"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.Int("size", len(fileInfos)),
	)

	for _, fileInfo := range fileInfos {

		zap.L().Debug("ReadDirAll File Response",
			zap.String("op", "readdirall"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", path.Join(rn.RemotePath.Path, fileInfo.Name)),
			zap.Uint64("size", fileInfo.Size),
			zap.String("mode", os.FileMode(fileInfo.Mode).String()),
			zap.Time("mtime", time.Unix(0, int64(fileInfo.Mtime))),
			zap.Time("atime", time.Unix(0, int64(fileInfo.Atime))),
		)

		// TODO Remove Remote Nodes if Missing
		val, ok := rn.RemoteNodes.Load(fileInfo.Name)

		var fileNode *RemoteNode
		if !ok {
			fileNode = rn.generateChildRemoteNode(fileInfo.Name, fileInfo.IsDir)
		} else {
			fileNode = val.(*RemoteNode)
			FuseServer().InvalidateNodeData(fileNode)
		}

		mtime := time.Unix(0, int64(fileInfo.Mtime))
		atime := time.Unix(0, int64(fileInfo.Atime))

		// TODO Fix this
		if ok && mtime != fileNode.Mtime {
			//Hoarder().CacheFetch(rn.RemotePath)
		}

		fileNode.Size = uint64(fileInfo.Size)
		fileNode.Mode = os.FileMode(fileInfo.Mode)
		fileNode.IsCached = true
		fileNode.Mtime = mtime
		fileNode.Atime = atime
		fileNodes.Store(fileInfo.Name, fileNode)
		//rn.RemoteNodes[fileInfo.Name] = fileNode
	}

	rn.RemoteNodes = fileNodes
	return nil
}

func (rn *RemoteNode) Lookup(ctx context.Context, name string) (fs.Node, error) {

	zap.L().Debug("Lookup FS Request",
		zap.String("op", "lookup"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("name", name),
	)

	val, ok := rn.RemoteNodes.Load(name)

	if !ok {
		err := rn.updateChildrenRemoteNodes()
		if err != nil {
			return nil, err
		}
	}

	val, ok = rn.RemoteNodes.Load(name)

	zap.L().Debug("Lookup Response",
		zap.String("op", "lookup"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("name", name),
		zap.Bool("ok", ok),
	)

	if ok {
		return val.(fs.Node), nil
	} else {
		return nil, fuse.ENOENT
	}

}

func (rn *RemoteNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {

	zap.L().Debug("Open FS Request",
		zap.String("op", "open"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("flags", req.Flags.String()),
	)

	if rn.IsDir {
		return &FileHandle{RemoteNode: rn, Fd: 0}, nil
	}

	fd, err := rn.CacheManager.Open(rn.RemotePath, uint32(req.Flags))
	//fd, err = FileHandler().OpenFile(rn.RemotePath, req.Flags, rn.IsDir)

	if err != nil {

		zap.L().Debug("Open Error Response",
			zap.String("op", "open"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.String("flags", req.Flags.String()),
			zap.Error(err),
		)

		return nil, err
	}

	fh := &FileHandle{
		RemoteNode: rn,
		Fd:         fd,
	}

	return fh, nil

}

func (rn *RemoteNode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	// TODO Add other attributes

	zap.L().Debug("SetAttr FS Request",
		zap.String("op", "setattr"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("valid", req.Valid.String()),
		zap.Uint64("size", req.Size),
		zap.String("mode", req.Mode.String()),
		zap.Time("atime", req.Atime),
		zap.Time("mtime", req.Mtime),
	)

	if req.Valid.Size() {
		err := rn.CacheManager.Truncate(rn.RemotePath, req.Size)

		if err != nil {

			zap.L().Warn("SetAttr Error Response",
				zap.String("op", "setattr"),
				zap.String("address", rn.RemotePath.Address()),
				zap.String("path", rn.RemotePath.Path),
				zap.String("valid", req.Valid.String()),
				zap.Uint64("size", req.Size),
				zap.String("mode", req.Mode.String()),
				zap.Time("atime", req.Atime),
				zap.Time("mtime", req.Mtime),
				zap.Error(err),
			)

			return err
		}

		rn.Size = req.Size

	} else if req.Valid.Mode() {

		err := rn.CacheManager.SetMode(rn.RemotePath, uint32(req.Mode))

		if err != nil {

			zap.L().Warn("SetAttr Error Response",
				zap.String("op", "setattr"),
				zap.String("address", rn.RemotePath.Address()),
				zap.String("path", rn.RemotePath.Path),
				zap.String("valid", req.Valid.String()),
				zap.Uint64("size", req.Size),
				zap.String("mode", req.Mode.String()),
				zap.Time("atime", req.Atime),
				zap.Time("mtime", req.Mtime),
				zap.Error(err),
			)

			return err
		}

		rn.Mode = req.Mode
	} else if req.Valid.Mtime() || req.Valid.Atime() {

		err := rn.CacheManager.SetMtime(rn.RemotePath, uint64(req.Mtime.UnixNano()), uint64(req.Atime.UnixNano()))

		if err != nil {

			zap.L().Warn("SetAttr Error Response",
				zap.String("op", "setattr"),
				zap.String("address", rn.RemotePath.Address()),
				zap.String("path", rn.RemotePath.Path),
				zap.String("valid", req.Valid.String()),
				zap.Uint64("size", req.Size),
				zap.String("mode", req.Mode.String()),
				zap.Time("atime", req.Atime),
				zap.Time("mtime", req.Mtime),
				zap.Error(err),
			)

			return err
		}

		rn.Mtime = req.Mtime
	}

	return nil
}

//// TODO Remove Fsync
//func (rn *RemoteNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
//
//	zap.L().Debug("Fsync FS Request",
//		zap.String("op", "fsync"),
//		zap.String("address", rn.RemotePath.Address()),
//		zap.String("path", rn.RemotePath.Path),
//	)
//
//	return nil
//}
//
func (rn *RemoteNode) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {

	zap.L().Debug("Create FS Request",
		zap.String("op", "create"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("name", req.Name),
	)

	// Create File Remotely
	// Create File in Cache if Space is available
	// File should be in open state
	// Return Errors
	fd, err := rn.CacheManager.Create(rn.RemotePath, req.Name)

	if err != nil {
		zap.L().Warn("Create Error Response",
			zap.String("op", "create"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.String("name", req.Name),
			zap.Error(err),
		)

		return nil, nil, err
	}

	fileNode := rn.generateChildRemoteNode(req.Name, false)
	rn.RemoteNodes.Store(req.Name, fileNode)

	fh := &FileHandle{
		Fd:         fd,
		RemoteNode: fileNode,
	}

	return fileNode, fh, nil
}

func (rn *RemoteNode) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {

	zap.L().Debug("Mkdir FS Request",
		zap.String("op", "mkdir"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("name", req.Name),
	)

	err := rn.CacheManager.Mkdir(rn.RemotePath, req.Name)

	if err != nil {
		zap.L().Warn("Mkdir Error Response",
			zap.String("op", "mkdir"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.String("name", req.Name),
			zap.Error(err),
		)

		return nil, err
	}

	fileNode := rn.generateChildRemoteNode(req.Name, true)
	rn.RemoteNodes.Store(req.Name, fileNode)
	return fileNode, nil
}

func (rn *RemoteNode) Remove(ctx context.Context, req *fuse.RemoveRequest) error {

	zap.L().Debug("Remove FS Request",
		zap.String("op", "remove"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("name", req.Name),
	)

	rp := &structure.RemotePath{
		Hostname: rn.RemotePath.Hostname,
		Port:     rn.RemotePath.Port,
		Path:     path.Join(rn.RemotePath.Path, req.Name),
	}

	err := rn.CacheManager.Remove(rp, req.Dir)

	if err != nil {
		zap.L().Warn("Remove Error Response",
			zap.String("op", "remove"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.String("name", req.Name),
			zap.Error(err),
		)

		return err
	}

	rn.RemoteNodes.Delete(req.Name)
	return nil
}

func (rn *RemoteNode) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {

	zap.L().Debug("Rename FS Request",
		zap.String("op", "rename"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("old_name", req.OldName),
		zap.String("new_dir", newDir.(*RemoteNode).RemotePath.Path),
		zap.String("new_name", req.NewName),
	)

	rnDestDir := newDir.(*RemoteNode)
	val, ok := rn.RemoteNodes.Load(req.OldName)

	var curRn *RemoteNode
	if ok {
		curRn = val.(*RemoteNode)
	} else {
		return os.ErrNotExist
	}

	destPath := path.Join(rnDestDir.RemotePath.Path, req.NewName)

	err := rn.CacheManager.Rename(curRn.RemotePath, destPath)

	if err != nil {
		zap.L().Warn("Rename Error Response",
			zap.String("op", "rename"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.String("old_name", req.OldName),
			zap.String("new_dir", newDir.(*RemoteNode).RemotePath.Path),
			zap.String("new_name", req.NewName),
			zap.Error(err),
		)

		return err
	}

	// Check If destination exists (actual move should do it)
	// Do Move at Remote
	// Update Cache Map
	// Update Open Map
	// Change RemoteNode Path
	// Add RemoteNode in newDir's list (if doesnt exist)

	curRn.RemotePath.Path = destPath
	rn.RemoteNodes.Delete(req.OldName)
	rnDestDir.RemoteNodes.Store(req.NewName, curRn)

	return nil
}
