package ifs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"path/filepath"
	"strings"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func generateVirtualNodes(ifs *Ifs, paths []string, remotePaths []*RemotePath) (map[string]fs.Node) {

	aggPaths := make(map[string][]string)
	aggRemotePaths := make(map[string][]*RemotePath)
	virtualNodes := make(map[string]fs.Node)

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
			virtualNodes[k] = &VirtualNode{
				Ifs:   ifs,
				Nodes: generateVirtualNodes(ifs, v, aggRemotePaths[k]),
			}
		} else {
			virtualNodes[k] = &RemoteNode{
				Ifs:         ifs,
				IsDir:       true,
				RemotePath:  aggRemotePaths[k][0],
				RemoteNodes: make(map[string]*RemoteNode),
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

func generateRemoteRoots(ifs *Ifs, remoteRoots []*RemoteRoot) map[string]fs.Node {

	virtualNodes := make(map[string]fs.Node)

	for _, remoteRoot := range remoteRoots {
		vn := generateRemoteRoot(ifs, remoteRoot.Paths, remoteRoot.RemotePaths())
		virtualNodes[remoteRoot.Hostname] = vn
	}

	return virtualNodes
}

func SetupLogger(cfg *LogConfig) {

	loggerCfg := zap.NewDevelopmentConfig()

	if !cfg.Logging {
		logger := zap.NewNop()
		zap.ReplaceGlobals(logger)
		return
	} else if !cfg.Console {
		loggerCfg.OutputPaths = []string{cfg.Path}
		loggerCfg.ErrorOutputPaths = []string{cfg.Path}
	}

	if cfg.Debug {
		loggerCfg.Level.SetLevel(zapcore.DebugLevel)
	} else {
		loggerCfg.Level.SetLevel(zapcore.InfoLevel)
	}

	logger, _ := loggerCfg.Build()

	zap.ReplaceGlobals(logger)

}

func MountRemoteRoots(cfg *FsConfig) {

	// TODO Figure out more options to add
	options := []fuse.MountOption{
		fuse.NoAppleDouble(),
		fuse.NoAppleXattr(),
		fuse.VolumeName("IFS Volume"),
	}

	c, err := fuse.Mount(cfg.MountPoint, options...)
	if err != nil {
		zap.L().Fatal("Mount Failed",
			zap.Error(err),
		)
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
		zap.L().Fatal("Mount Failed",
			zap.Error(err),
		)
	}

	c.Close()
}
