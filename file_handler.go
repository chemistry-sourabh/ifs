package arsyncfs

import (
	"io/ioutil"
	"path"
	"log"
	"os"
	"strings"
	"io"
)

type FileHandler struct {
	Ifs    *Ifs
	Path   string
	Size   uint64
	Cached map[string]bool
	Opened map[string]bool
	//RequestChannel       chan *CacheRequest
	//EgressRequestChannel chan *Request
}

func (fh *FileHandler) StartUp() {
	log.Println("Starting File Handler")
	fh.DeleteCache()

	fh.Cached = make(map[string] bool)
	fh.Opened = make(map[string] bool)

}

func (fh *FileHandler) DeleteCache() {
	log.Println("Deleting Cache")
	os.RemoveAll(fh.Path)
	os.MkdirAll(fh.Path, 0755)
}

//func (c *FileHandler) ProcessRequests() {
//
//	for creq := range c.RequestChannel {
//		log.Println("Got FileHandler Request")
//
//		switch creq.Op {
//
//		case FetchFileCacheOp:
//			respChannel := make(chan BaseResponse)
//
//			req := &Request{
//				Op:              FetchFileRequest,
//				RemoteNode:      creq.RemoteNode,
//				ResponseChannel: respChannel,
//			}
//
//			log.Println("Forwarding FileHandler Request")
//			c.EgressRequestChannel <- req
//
//			c.ReceiveFile(respChannel)
//
//		case GetLocalFileCacheOp:
//
//			filename := fmt.Sprintf("%d", c.Map[creq.RemoteNode.RemotePath.String()])
//			Chunk, err := ioutil.ReadFile(path.Join(c.Path, filename))
//
//			if err != nil {
//				log.Fatal(err)
//			}
//
//			creq.ResponseChannel <- Chunk
//
//		}
//	}
//
//}

//func (c *FileHandler) ReceiveFile(respChannel chan BaseResponse) {
//
//	//resp := <-respChannel
//	//
//	//Chunk, _ := base64.StdEncoding.DecodeString(resp.(string))
//	//
//	//RequestId := c.CacheId
//	//c.CacheId++
//	//
//	//err := ioutil.WriteFile(path.Join(c.Path, fmt.Sprintf("%d", RequestId)), Chunk, 0666)
//	//
//	//if err != nil {
//	//	log.Fatal(err)
//	//}
//	//
//	//c.Map[resp.RNode.RemotePath.String()] = RequestId
//
//}

func (fh *FileHandler) OpenFile(remotePath *RemotePath) error {

	if fh.checkCacheSpace() {
		resp := fh.Ifs.Talker.sendRequest(FetchFileRequest, remotePath).Data.(*FileChunk).Chunk

		err := ioutil.WriteFile(path.Join(fh.Path, fh.convertToCacheName(remotePath)), resp,
			0666)

		if err != nil {
			log.Fatal(err)
		}

		fh.Cached[remotePath.String()] = true

	} else {
		// Should Do Something on Remote only, but nothing happens here
	}

	fh.Opened[remotePath.String()] = true

	return nil
}

func (fh *FileHandler) checkCacheSpace() bool {
	return true
}

func (fh *FileHandler) convertToCacheName(path *RemotePath) string {
	s := strings.Replace(path.String(), "/", "_", -1)
	s = strings.Replace(s, ":", "_", 1)
	s = strings.Replace(s, "@", "_", 1)
	return s
}

func (fh *FileHandler) ReadData(remotePath *RemotePath, offset int64, size int) ([]byte, int, error) {

	// TODO  Check if File is Open
	if _, ok := fh.Cached[remotePath.String()]; ok {
		f, err := os.OpenFile(path.Join(fh.Path, fh.convertToCacheName(remotePath)), os.O_RDONLY, 0666)

		if err != nil {
			log.Fatal(err)
		}

		b := make([]byte, size)
		n, err := f.ReadAt(b, offset)

		if err != nil {

			if err != io.EOF {
				log.Fatal(err)
			}
		}

		f.Close()

		return b, n, err
	} else {
	// Should Ask Agent for bytes



	}


	return nil, 0, nil
}

func (fh *FileHandler) WriteData(remoteNode *RemoteNode, b []byte, offset int64) int {

	n := 0

	if _, ok := fh.Cached[remoteNode.RemotePath.String()]; ok {
		f, err := os.OpenFile(path.Join(fh.Path, fh.convertToCacheName(remoteNode.RemotePath)), os.O_RDONLY, 0666)

		log.Println(path.Join(fh.Path, fh.convertToCacheName(remoteNode.RemotePath)))

		if err != nil {
			log.Fatal(err)
		}

		n, err = f.WriteAt(b, offset)

		if err != nil {
			log.Fatal(err)
		}

		f.Close()

	}

	// Send Bytes to Agent

	return n
}
