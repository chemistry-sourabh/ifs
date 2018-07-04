package ifs

import (
	"bazil.org/fuse/fs"
	"bazil.org/fuse"
	"os/user"
	"strconv"
	"os"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

type VirtualNode struct {
	Ifs   *Ifs
	Nodes map[string] fs.Node
}

func (vn *VirtualNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	log.WithFields(log.Fields{"vn": true, "op": "attr"}).Debug("Attr FS Request")

	curUser, _ := user.Current()
	uid, _ := strconv.ParseUint(curUser.Uid, 10, 64)

	curGroup, _ := user.LookupGroup("staff")
	gid, _ := strconv.ParseUint(curGroup.Gid, 10, 64)

	attr.Uid = uint32(uid)
	attr.Gid = uint32(gid)
	//attr.Size = uint64(10)
	attr.Mode = os.FileMode(os.ModeDir | 0755)

	return nil
}

func (vn *VirtualNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.WithFields(log.Fields{"vn": true, "op": "readdir"}).Debug("ReadDir FS Request")

	var children []fuse.Dirent

	for dirName := range vn.Nodes {
		child := fuse.Dirent{Type: fuse.DT_Dir, Name: dirName}
		children = append(children, child)
	}

	return children, nil
}

func (vn *VirtualNode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.WithFields(log.Fields{"vn": true, "op": "lookup", "name": name}).Debug("Lookup FS Request")

	val, ok := vn.Nodes[name]

	if ok {
		return val, nil
	} else {
		return nil, fuse.ENOENT
	}
}
