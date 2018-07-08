package ifs

import (
	"bazil.org/fuse"
	log "github.com/sirupsen/logrus"
	"bazil.org/fuse/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func generateVirtualNodes(ifs *Ifs, paths []string, remotePaths []*RemotePath) (map[string]fs.Node) {

	aggPaths := make(map[string][]string)
	aggRemotePaths := make(map[string] []*RemotePath)
	virtualNodes := make(map[string]fs.Node)

	for i, p := range paths {


		l := strings.Split(strings.Trim(p, "/") , "/")

		if l[0] != "" {
			firstDir := l[0]
			aggPaths[firstDir] = append(aggPaths[firstDir], filepath.Join(l[1:]...))
			aggRemotePaths[firstDir] = append(aggRemotePaths[firstDir], remotePaths[i])
		}

	}

	for k, v := range aggPaths {

		if len(v) > 1 || (len(v) == 1 && v[0] != "" ){
			virtualNodes[k] = &VirtualNode{
				Ifs:   ifs,
				Nodes: generateVirtualNodes(ifs, v, aggRemotePaths[k]),
			}
		} else {
			virtualNodes[k] = &RemoteNode{
				Ifs: ifs,
				IsDir: true,
				RemotePath: aggRemotePaths[k][0],
				RemoteNodes: make(map[string] *RemoteNode),
			}
		}
	}

	return virtualNodes
}

func generateRemoteRoot(ifs *Ifs, paths []string, remotePaths []*RemotePath) *VirtualNode {

	return &VirtualNode{
		Ifs:   ifs,
		Nodes: generateVirtualNodes(ifs, paths, remotePaths),
	}
}

func generateRemoteRoots(ifs *Ifs, remoteRoots []*RemoteRoot) map[string] fs.Node {

	virtualNodes := make(map[string] fs.Node)

	for _, remoteRoot := range remoteRoots {
		vn := generateRemoteRoot(ifs, remoteRoot.Paths, remoteRoot.RemotePaths())
		virtualNodes[remoteRoot.Hostname] = vn
	}

	return virtualNodes
}

func SetupLogger(cfg *LogConfig) {
	if !cfg.Logging {
		log.SetOutput(ioutil.Discard)
	} else if !cfg.Console {
		f, _ := os.Create(cfg.Path)
		//defer f.Close()
		log.SetOutput(f)
	}

	formatter := &log.TextFormatter{}
	formatter.DisableColors = true

	log.SetFormatter(formatter)

	if cfg.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

}

func MountRemoteRoots(cfg *FsConfig) {
	c, err := fuse.Mount(cfg.MountPoint)
	if err != nil {
		log.Fatal(err)
	}

	server := fs.New(c, nil)


	fileSystem := &Ifs{
		Server: server,
	}

	remoteRootNodes := generateRemoteRoots(fileSystem, cfg.RemoteRoots)

	talker := NewTalker(fileSystem)

	fileHandler := &FileHandler{
		Ifs: fileSystem,
	}

	hoarder := &Hoarder{
		Ifs:  fileSystem,
		Path: cfg.CacheLocation,
		Size: 100,
	}

	fileSystem.Talker = talker
	fileSystem.FileHandler = fileHandler
	fileSystem.Hoarder = hoarder
	fileSystem.RemoteRoots = remoteRootNodes

	talker.Startup(cfg.RemoteRoots, cfg.ConnCount)
	hoarder.Startup()
	fileHandler.StartUp()

	server.Serve(fileSystem)

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Panicln(err)
	}

	c.Close()
}
