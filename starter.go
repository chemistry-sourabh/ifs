package ifs

import (
	"bazil.org/fuse"
	log "github.com/sirupsen/logrus"
	"bazil.org/fuse/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"fmt"
	"strings"
)

func generateVirtualNodes(ifs *Ifs, paths []string) (map[string]fs.Node) {

	aggPaths := make(map[string][]string)
	virtualNodes := make(map[string]fs.Node)

	for _, p := range paths {


		l := strings.Split(strings.Trim(p, "/") , "/")
		//l := filepath.SplitList(p)

		if l[0] != "" {
			firstDir := l[0]
			aggPaths[firstDir] = append(aggPaths[firstDir], filepath.Join(l[1:]...))
		}

		//if _, ok := virtualNodes[firstDir]; !ok {
		//	virtualNodes[firstDir] = generateVirtualNode(ifs, p)
		//} else {
		//
		//}

	}

	for k, v := range aggPaths {

		if len(v) > 0 {
			virtualNodes[k] = &VirtualNode{
				Ifs:   ifs,
				Nodes: generateVirtualNodes(ifs, v),
			}
		} else {
			virtualNodes[k] = &RemoteNode{
				Ifs: ifs,
				IsDir: true,
				//RemotePath:
				RemoteNodes: make(map[string] *RemoteNode),
			}
		}
	}

	return virtualNodes
}

func generateRemoteRoot(ifs *Ifs, paths []string) *VirtualNode {

	return &VirtualNode{
		Ifs:   ifs,
		Nodes: generateVirtualNodes(ifs, paths),
	}
}

func generateRemoteRoots(ifs *Ifs, remoteRoots []*RemoteRoot) map[string] fs.Node {

	virtualNodes := make(map[string] fs.Node)

	for _, remoteRoot := range remoteRoots {
		vn := generateRemoteRoot(ifs, remoteRoot.Paths)
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
		CachedStats: make(map[string]*Stat),
	}

	fmt.Println(cfg.RemoteRoots)
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

	for _, rr := range cfg.RemoteRoots {
		talker.Startup(rr.Address(), cfg.ConnCount)
	}
	hoarder.Startup()
	fileHandler.StartUp()

	server.Serve(fileSystem)

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Panicln(err)
	}

	c.Close()
}
