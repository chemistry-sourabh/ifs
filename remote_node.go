package ifs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"path"
	"time"
	"os/user"
	"strconv"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"os"
)

type RemoteNode struct {
	RemotePath *RemotePath

	IsDir    bool
	IsCached bool
	Size     uint64
	Mode     os.FileMode
	Mtime    time.Time
	// TODO Add Atime also

	// Children
	RemoteNodes map[string]*RemoteNode
}

func (rn *RemoteNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	// Update fileHandler if invalid

	zap.L().Debug("Attr FS Request",
		zap.String("op", "attr"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
	)

	if !rn.IsCached {

		var resp *Packet
		resp = Talker().sendRequest(AttrRequest, rn.RemotePath.Hostname, rn.RemotePath)

		var err error = nil
		if respErr, ok := resp.Data.(Error); !ok {

			s := resp.Data.(*Stat)

			zap.L().Debug("Attr Response From Agent",
				zap.String("op", "attr"),
				zap.String("address", rn.RemotePath.Address()),
				zap.String("path", rn.RemotePath.Path),
				zap.String("mode", s.Mode.String()),
				zap.Int64("size", s.Size),
				zap.Time("mtime", time.Unix(0, s.ModTime)),
			)

			rn.Size = uint64(s.Size)
			rn.Mode = s.Mode
			rn.Mtime = time.Unix(0, s.ModTime)
			rn.IsCached = true

		} else {
			err = respErr.Err

			zap.L().Warn("Attr Error Response",
				zap.String("op", "attr"),
				zap.String("address", rn.RemotePath.Address()),
				zap.String("path", rn.RemotePath.Path),
				zap.Error(err),
			)

			return err
		}

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
	//attr.Valid = time.Duration(1)

	zap.L().Debug("Attr Response",
		zap.String("op", "attr"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("mode", rn.Mode.String()),
		zap.Uint64("size", rn.Size),
		zap.Time("mtime", rn.Mtime),
	)

	return nil
}

func (rn *RemoteNode) generateChildRemoteNode(name string, isDir bool) *RemoteNode {
	return &RemoteNode{
		IsDir:    isDir,
		IsCached: false,
		RemotePath: &RemotePath{
			Hostname: rn.RemotePath.Hostname,
			Port:     rn.RemotePath.Port,
			Path:     path.Join(rn.RemotePath.Path, name),
		},
		RemoteNodes: make(map[string]*RemoteNode),
	}
}

func (rn *RemoteNode) updateChildrenRemoteNodes() {
	resp := Talker().sendRequest(ReadDirAllRequest, rn.RemotePath.Hostname, rn.RemotePath)

	zap.L().Debug("ReaddirAll FS Request",
		zap.String("op", "readdirall"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
	)

	//rn.RemoteNodes = make(map[string]*RemoteNode)

	var err error
	if respError, ok := resp.Data.(Error); !ok {

		files := resp.Data.(*DirInfo).Stats

		zap.L().Debug("ReaddirAll Response from Agent",
			zap.String("op", "readdirall"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.Int("size", len(files)),
		)

		for _, file := range files {
			s := file

			zap.L().Debug("ReadDirAll File Response",
				zap.String("op", "readdirall"),
				zap.String("address", rn.RemotePath.Address()),
				zap.String("path", path.Join(rn.RemotePath.Path, s.Name)),
				zap.Int64("size", s.Size),
				zap.String("mode", s.Mode.String()),
				zap.Time("mtime", time.Unix(0, s.ModTime)),
			)

			// TODO Remove Remote Nodes if Missing
			newRn, ok := rn.RemoteNodes[s.Name]

			if !ok {
				newRn = rn.generateChildRemoteNode(s.Name, s.IsDir)
				rn.RemoteNodes[s.Name] = newRn
			}

			newRn.Size = uint64(s.Size)
			newRn.Mode = s.Mode
			newRn.Mtime = time.Unix(0, s.ModTime)
			newRn.IsCached = true

			//rn.RemoteNodes[s.Name] = newRn
		}

	} else {
		err = respError.Err

		zap.L().Warn("ReadDirAll Error Response",
			zap.String("op", "readdirall"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.Error(err),
		)

	}
}

func (rn *RemoteNode) Lookup(ctx context.Context, name string) (fs.Node, error) {

	zap.L().Debug("Lookup FS Request",
		zap.String("op", "lookup"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("name", name),
	)

	val, ok := rn.RemoteNodes[name]

	if !ok {
		rn.updateChildrenRemoteNodes()
	}

	val, ok = rn.RemoteNodes[name]

	zap.L().Debug("Lookup Response",
		zap.String("op", "lookup"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("name", name),
		zap.Bool("ok", ok),
	)

	if ok {
		return val, nil
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

	var err error
	var fd uint64

	fd, err = FileHandler().OpenFile(rn.RemotePath, req.Flags, rn.IsDir)

	if err != nil {

		zap.L().Debug("Open Error Response",
			zap.String("op", "open"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.String("flags", req.Flags.String()),
			zap.Error(err),
		)

	}

	fh := &FileHandle{
		RemoteNode:     rn,
		FileDescriptor: fd,
	}

	return fh, err

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

	attrInfo := &AttrInfo{
		Path:  rn.RemotePath.Path,
		Valid: req.Valid,
		Size:  req.Size,
		Mode:  req.Mode,
		ATime: req.Atime.UnixNano(),
		MTime: req.Mtime.UnixNano(),
	}

	var err error
	if req.Valid.Size() {
		err = FileHandler().Truncate(rn.RemotePath, attrInfo)

		if err == nil {
			rn.Size = req.Size
		}

	} else {
		resp := Talker().sendRequest(SetAttrRequest, rn.RemotePath.Hostname, attrInfo)
		if respErr, ok := resp.Data.(Error); ok {
			err = respErr.Err
		}

		if err == nil {

			if req.Valid.Mode() {
				rn.Mode = req.Mode
			} else if req.Valid.Mtime() {
				rn.Mtime = req.Mtime
			}

		}
	}

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
	}

	return err
}

// TODO Remove Fsync
func (rn *RemoteNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {

	zap.L().Debug("Fsync FS Request",
		zap.String("op", "fsync"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
	)

	return nil
}

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
	fd, err := FileHandler().Create(rn.RemotePath, req.Name)
	if err == nil {
		newRn := rn.generateChildRemoteNode(req.Name, false)
		rn.RemoteNodes[req.Name] = newRn

		fh := &FileHandle{
			FileDescriptor: fd,
			RemoteNode:     newRn,
		}

		return newRn, fh, nil
	} else {

		zap.L().Warn("Create Error Response",
			zap.String("op", "create"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.String("name", req.Name),
			zap.Error(err),
		)

	}

	return nil, nil, err
}

func (rn *RemoteNode) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {

	zap.L().Debug("Mkdir FS Request",
		zap.String("op", "mkdir"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("name", req.Name),
	)

	err := FileHandler().Mkdir(rn.RemotePath, req.Name)

	if err == nil {
		newRn := rn.generateChildRemoteNode(req.Name, true)
		rn.RemoteNodes[req.Name] = newRn
		return newRn, nil
	} else {

		zap.L().Warn("Mkdir FS Response",
			zap.String("op", "mkdir"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.String("name", req.Name),
			zap.Error(err),
		)

	}

	return nil, err
}

func (rn *RemoteNode) Remove(ctx context.Context, req *fuse.RemoveRequest) error {

	zap.L().Debug("Remove FS Request",
		zap.String("op", "remove"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("name", req.Name),
	)

	err := FileHandler().Remove(rn.RemotePath, req.Name, rn.IsDir)
	if err == nil {
		delete(rn.RemoteNodes, req.Name)
	} else {

		zap.L().Warn("Remove Error Response",
			zap.String("op", "remove"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.String("name", req.Name),
			zap.Error(err),
		)

	}
	return err
}

func (rn *RemoteNode) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {

	zap.L().Debug("Rename FS Request",
		zap.String("op", "rename"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.String("old_name", req.OldName),
		zap.String("new_name", req.NewName),
		zap.String("new_dir", newDir.(*RemoteNode).RemotePath.Path),
	)

	rnDestDir := newDir.(*RemoteNode)
	curRn := rn.RemoteNodes[req.OldName]
	destPath := path.Join(rnDestDir.RemotePath.Path, req.NewName)

	err := FileHandler().Rename(curRn.RemotePath, destPath)
	// Check If destination exists (actual move should do it)
	// Do Move at Remote
	// Update Cache Map
	// Update Open Map
	// Change RemoteNode Path
	// Add RemoteNode in newDir's list (if doesnt exist)

	if err == nil {
		curRn.RemotePath.Path = destPath
		delete(rn.RemoteNodes, req.OldName)
		rnDestDir.RemoteNodes[req.NewName] = curRn
	} else {

		zap.L().Warn("Rename Error Response",
			zap.String("op", "rename"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.String("old_name", req.OldName),
			zap.String("new_name", req.NewName),
			zap.String("new_dir", newDir.(*RemoteNode).RemotePath.Path),
			zap.Error(err),
		)

	}

	return err
}
