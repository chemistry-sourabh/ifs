package ifs

import (
	"bazil.org/fuse"
	"golang.org/x/net/context"
	"bazil.org/fuse/fs"
	log "github.com/sirupsen/logrus"
	"os/user"
	"strconv"
	"os"
	"time"
)

type Ifs struct {
	Talker      *Talker
	FileHandler *FileHandler
	Hoarder     *Hoarder
	RemoteRoots map[string] fs.Node
}

// TODO All Errors should be resolved here
func (root *Ifs) Root() (fs.Node, error) {
	return root, nil
}

func (root *Ifs) Attr(ctx context.Context, attr *fuse.Attr) error {
	log.WithFields(log.Fields{"root": true, "op": "attr"}).Debug("Attr FS Request")
	// Check Error
	curUser, _ := user.Current()
	uid, _ := strconv.ParseUint(curUser.Uid, 10, 64)

	curGroup, _ := user.LookupGroup("staff")
	gid, _ := strconv.ParseUint(curGroup.Gid, 10, 64)

	attr.Uid = uint32(uid)
	attr.Gid = uint32(gid)
	//attr.Size = uint64(10)
	attr.Mode = os.FileMode(os.ModeDir | 0755)
	//attr.Mtime = s.ModTime

	return nil
}

func (root *Ifs) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.WithFields(log.Fields{"root": true, "op": "readdir"}).Debug("ReadDir FS Request")

	var children []fuse.Dirent

	for dirName := range root.RemoteRoots {
		child := fuse.Dirent{Type: fuse.DT_Dir, Name: dirName}
		children = append(children, child)
	}

	return children, nil
}

func (root *Ifs) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.WithFields(log.Fields{"root": true, "op": "lookup", "name": name}).Debug("Lookup FS Request")

	val, ok := root.RemoteRoots[name]

	if ok {
		return val, nil
	} else {
		return nil, fuse.ENOENT
	}
}

// TODO Should Return Error
func (root *Ifs) UpdateAttr(hostname string, info *AttrUpdateInfo) error {
	remoteRoot := root.RemoteRoots[hostname].(*VirtualNode)

	rn := findNode(remoteRoot, info.Path)

	rn.Size = uint64(info.Size)
	rn.Mode = info.Mode
	rn.Mtime = time.Unix(0, info.ModTime)

	log.WithFields(log.Fields{
		"hostname": hostname,
		"path": info.Path,
		"node_path": rn.RemotePath.String(),
		"size": rn.Size,
		"mode": rn.Mode,
		"mtime": rn.Mtime,
	}).Debug("Updated Remote Node")

	return nil
}

func findNode(node fs.Node, nodePath string) *RemoteNode {

	if nodePath == "" {
		return node.(*RemoteNode)
	}

	firstDir := FirstDir(nodePath)
	restPath := RemoveFirstDir(nodePath)

	switch n := node.(type) {
	case *VirtualNode:
		newNode := n.Nodes[firstDir]
		return findNode(newNode, restPath)
	case *RemoteNode:
		newNode := n.RemoteNodes[firstDir]
		return findNode(newNode, restPath)
	}

	//child := vn.Nodes[path]
	return nil
}

