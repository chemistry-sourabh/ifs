package ifs

import (
	"go.uber.org/zap"
	"time"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// TODO Should Return Error
func UpdateAttr(hostname string, info *AttrUpdateInfo) error {

	zap.L().Debug("Update Attr Request",
		zap.String("op", "attrupdate"),
		zap.String("hostname", hostname),
		zap.String("path", info.Path),
		zap.String("mode", info.Mode.String()),
		zap.Int64("size", info.Size),
		zap.Time("mod_time", time.Unix(0, info.ModTime)),
	)

	val, ok := Ifs().RemoteRoots.Get(hostname)

	var remoteRoot *VirtualNode

	if ok {
		remoteRoot = val.(*VirtualNode)
	}

	rn := findNode(remoteRoot, info.Path)

	err := FuseServer().InvalidateNodeData(rn)

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
		val, ok := n.Nodes.Get(firstDir)
		if ok {
			return findNode(val.(fs.Node), restPath)
		}
	case *RemoteNode:
		val, ok := n.RemoteNodes.Get(firstDir)
		if ok {
			return findNode(val.(*RemoteNode), restPath)
		}
	}

	return nil
}

