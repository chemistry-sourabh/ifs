package ifs

import (
	"bazil.org/fuse"
	"golang.org/x/net/context"
	"bazil.org/fuse/fs"
	"os/user"
	"strconv"
	"os"
	"go.uber.org/zap"
	"sync"
	"github.com/orcaman/concurrent-map"
	"strings"
	"path/filepath"
)

type fileSystem struct {
	RemoteRoots cmap.ConcurrentMap
}

var(
	fileSystemInstance *fileSystem
	fileSystemOnce sync.Once
)

func Ifs() *fileSystem {
	fileSystemOnce.Do(func() {
		fileSystemInstance = &fileSystem{
			RemoteRoots: cmap.New(),
		}
	})

	return fileSystemInstance
}

func (root *fileSystem) Startup(remoteRoots []*RemoteRoot) {
	root.RemoteRoots = generateRemoteRoots(remoteRoots)
}

// TODO All Errors should be resolved here
func (root *fileSystem) Root() (fs.Node, error) {
	return root, nil
}

func (root *fileSystem) Attr(ctx context.Context, attr *fuse.Attr) error {

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

func (root *fileSystem) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {

	zap.L().Debug("ReadDir FS Request",
		zap.Bool("root", true),
		zap.String("op", "readdir"),
	)

	var children []fuse.Dirent

	for dirName := range root.RemoteRoots.IterBuffered() {
		child := fuse.Dirent{Type: fuse.DT_Dir, Name: dirName.Key}
		children = append(children, child)
	}

	zap.L().Debug("ReadDir Response",
		zap.Bool("root", true),
		zap.String("op", "readdir"),
		//zap.Strings("remote_roots", root.RemoteRoots),
	)

	return children, nil
}

func (root *fileSystem) Lookup(ctx context.Context, name string) (fs.Node, error) {

	zap.L().Debug("Lookup FS Request",
		zap.Bool("root", true),
		zap.String("op", "lookup"),
		zap.String("name", name),
	)

	val, ok := root.RemoteRoots.Get(name)


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

func generateVirtualNodes(paths []string, remotePaths []*RemotePath) (cmap.ConcurrentMap) {

	aggPaths := make(map[string][]string)
	aggRemotePaths := make(map[string][]*RemotePath)
	virtualNodes := cmap.New()

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
			virtualNodes.Set(k, &VirtualNode{
				Nodes: generateVirtualNodes(v, aggRemotePaths[k]),
			})
		} else {
			cm := cmap.New()
			virtualNodes.Set(k, &RemoteNode{
				IsDir:       true,
				RemotePath:  aggRemotePaths[k][0],
				RemoteNodes: &cm,
			})
		}
	}

	return virtualNodes
}

func generateRemoteRoot(paths []string, remotePaths []*RemotePath) *VirtualNode {

	return &VirtualNode{
		Nodes: generateVirtualNodes(paths, remotePaths),
	}
}

func generateRemoteRoots(remoteRoots []*RemoteRoot) cmap.ConcurrentMap {

	virtualNodes := cmap.New()

	for _, remoteRoot := range remoteRoots {
		vn := generateRemoteRoot(remoteRoot.Paths, remoteRoot.RemotePaths())
		virtualNodes.Set(remoteRoot.Hostname, vn)
	}

	return virtualNodes
}
