package ifs

import (
	"bazil.org/fuse"
	"golang.org/x/net/context"
	"bazil.org/fuse/fs"
	"os/user"
	"strconv"
	"os"
	"time"
	"go.uber.org/zap"
)

type Ifs struct {
	Server      *fs.Server
	Talker      *Talker
	FileHandler *FileHandler
	Hoarder     *Hoarder
	RemoteRoots map[string]fs.Node
}

// TODO All Errors should be resolved here
func (root *Ifs) Root() (fs.Node, error) {
	return root, nil
}

func (root *Ifs) Attr(ctx context.Context, attr *fuse.Attr) error {

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
	//attr.Mtime = s.ModTime

	zap.L().Debug("Attr Response",
		zap.Bool("root", true),
		zap.String("op", "attr"),
	)

	return nil
}

func (root *Ifs) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {

	zap.L().Debug("ReadDir FS Request",
		zap.Bool("root", true),
		zap.String("op", "readdir"),
	)

	var children []fuse.Dirent

	for dirName := range root.RemoteRoots {
		child := fuse.Dirent{Type: fuse.DT_Dir, Name: dirName}
		children = append(children, child)
	}

	zap.L().Debug("ReadDir Response",
		zap.Bool("root", true),
		zap.String("op", "readdir"),
		//zap.Strings("remote_roots", root.RemoteRoots),
	)

	return children, nil
}

func (root *Ifs) Lookup(ctx context.Context, name string) (fs.Node, error) {

	zap.L().Debug("Lookup FS Request",
		zap.Bool("root", true),
		zap.String("op", "lookup"),
		zap.String("name", name),
	)

	val, ok := root.RemoteRoots[name]

	zap.L().Debug("Lookup Response",
		zap.Bool("root", true),
		zap.String("op", "lookup"),
		zap.String("name", name),
		zap.Bool("ok", ok),
	)

	if ok {
		return val, nil
	} else {
		return nil, fuse.ENOENT
	}
}

// TODO Should Return Error
func (root *Ifs) UpdateAttr(hostname string, info *AttrUpdateInfo) error {

	zap.L().Debug("Update Attr Request",
		zap.String("op", "attrupdate"),
		zap.String("hostname", hostname),
		zap.String("path", info.Path),
		zap.String("mode", info.Mode.String()),
		zap.Int64("size", info.Size),
		zap.Time("mod_time", time.Unix(0, info.ModTime)),
	)

	remoteRoot := root.RemoteRoots[hostname].(*VirtualNode)

	rn := findNode(remoteRoot, info.Path)

	err := root.Server.InvalidateNodeData(rn)

	if err == fuse.ErrNotCached {
		zap.L().Warn("Node Not Cached",
			zap.String("op", "attrupdate"),
			zap.String("hostname", hostname),
			zap.String("path", info.Path),
			zap.String("mode", info.Mode.String()),
			zap.Int64("size", info.Size),
			zap.Time("mtime", time.Unix(0, info.ModTime)),
		)
	}

	if rn != nil {

		rn.Size = uint64(info.Size)
		rn.Mode = info.Mode
		rn.Mtime = time.Unix(0, info.ModTime)

		zap.L().Debug("Updated Attr",
			zap.String("op", "attrupdate"),
			zap.String("hostname", hostname),
			zap.String("path", info.Path),
			zap.String("node_path", rn.RemotePath.Path),
			zap.String("mode", rn.Mode.String()),
			zap.Uint64("size", rn.Size),
			zap.Time("mtime", rn.Mtime),
		)

	}

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
