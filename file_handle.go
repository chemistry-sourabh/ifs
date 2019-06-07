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
	"os"
	"path"
	"sync"
	"time"
)
import (
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type FileHandle struct {
	RemoteNode *RemoteNode
	Fd         uint64
}

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {

	rn := fh.RemoteNode

	zap.L().Debug("Read FS Request",
		zap.String("op", "read"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.Int64("offset", req.Offset),
		zap.Int("size", req.Size),
		zap.Uint64("fd", fh.Fd),
	)

	b, err := fh.RemoteNode.CacheManager.Read(fh.Fd, uint64(req.Offset), uint64(req.Size))

	if err != nil {

		zap.L().Warn("Read Error Response",
			zap.String("op", "read"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.Int64("offset", req.Offset),
			zap.Int("size", req.Size),
			zap.Uint64("fd", fh.Fd),
			zap.Error(err),
		)

		return err
	}

	resp.Data = b

	return nil
}

func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {

	rn := fh.RemoteNode

	zap.L().Debug("Write FS Request",
		zap.String("op", "write"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.Int64("offset", req.Offset),
		zap.Int("size", len(req.Data)),
	)

	n, err := fh.RemoteNode.CacheManager.Write(fh.Fd, uint64(req.Offset), req.Data)

	if err != nil {

		zap.L().Warn("Write Error Response",
			zap.String("op", "write"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.Int64("offset", req.Offset),
			zap.Int("size", len(req.Data)),
			zap.Error(err),
		)

		return err
	}

	resp.Size = n
	return nil
}

// TODO Remove Nodes if not present on remote
func (fh *FileHandle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {

	rn := fh.RemoteNode

	zap.L().Debug("ReadDir FS Request",
		zap.String("op", "readdir"),
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

		return nil, err
	}

	var children []fuse.Dirent

	zap.L().Debug("ReadDir Response From Agent",
		zap.String("op", "readdir"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.Int("size", len(fileInfos)),
	)

	fileNodes := &sync.Map{}

	for _, fileInfo := range fileInfos {


		zap.L().Debug("ReadDir File Response",
			zap.String("op", "readdir"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", path.Join(rn.RemotePath.Path, fileInfo.Name)),
			zap.Uint64("size", fileInfo.Size),
			zap.String("mode", os.FileMode(fileInfo.Mode).String()),
			zap.Time("mtime", time.Unix(0, int64(fileInfo.Mtime))),
		)

		var child fuse.Dirent
		if fileInfo.IsDir {
			child = fuse.Dirent{Type: fuse.DT_Dir, Name: fileInfo.Name}
		} else {
			child = fuse.Dirent{Type: fuse.DT_File, Name: fileInfo.Name}
		}
		children = append(children, child)

		val, ok := rn.RemoteNodes.Load(fileInfo.Name)

		var fileNode *RemoteNode

		if !ok {
			fileNode = rn.generateChildRemoteNode(fileInfo.Name, fileInfo.IsDir)
		} else {
			fileNode = val.(*RemoteNode)
			FuseServer().InvalidateNodeData(fileNode)
		}

		mtime := time.Unix(0, int64(fileInfo.Mtime))

		//TODO Fix This
		if ok && mtime != fileNode.Mtime {
			//Hoarder().CacheFetch(rn.RemotePath)
		}

		fileNode.Size = fileInfo.Size
		fileNode.Mode = os.FileMode(fileInfo.Mode)
		fileNode.Mtime = time.Unix(0, int64(fileInfo.Mtime))
		fileNode.IsCached = true

		fileNodes.Store(fileInfo.Name, fileNode)

	}

	//TODO Might be fishy (Atomic?)
	rn.RemoteNodes = fileNodes

	return children, nil
}

func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {

	rn := fh.RemoteNode

	zap.L().Debug("Flush FS Request",
		zap.String("op", "flush"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
	)

	err := rn.CacheManager.Flush(fh.Fd)

	if err != nil {
		zap.L().Warn("Flush Error Response",
			zap.String("op", "flush"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.Error(err),
		)
	}

	return err
}

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {

	rn := fh.RemoteNode

	zap.L().Debug("Release FS Request",
		zap.String("op", "release"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
	)

	if fh.Fd == 0 {
		return nil
	}

	err := rn.CacheManager.Close(fh.Fd)

	if err != nil {
		zap.L().Warn("Release Error Response",
			zap.String("op", "flush"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.Error(err),
		)
	}

	return err
}
