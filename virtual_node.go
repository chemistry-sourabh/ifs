package ifs

import (
	"bazil.org/fuse/fs"
	"bazil.org/fuse"
	"os/user"
	"strconv"
	"os"
	"golang.org/x/net/context"
	"go.uber.org/zap"
	"github.com/orcaman/concurrent-map"
	"time"
)

type VirtualNode struct {
	Nodes cmap.ConcurrentMap
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

	for dirName := range vn.Nodes.IterBuffered() {
		child := fuse.Dirent{Type: fuse.DT_Dir, Name: dirName.Key}
		children = append(children, child)
	}

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

	val, ok := vn.Nodes.Get(name)

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
