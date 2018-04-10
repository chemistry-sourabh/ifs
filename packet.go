package arsyncfs

import (
	"github.com/vmihailenco/msgpack"
	"log"
	"encoding/binary"
)

type Payload interface {
}

type Packet struct {
	Id   uint64
	Op   uint8 `json:"op"`
	Data Payload
}

func (pkt *Packet) Marshal() []byte {
	header := make([]byte, 9)
	binary.BigEndian.PutUint64(header, pkt.Id)
	header[8] = pkt.Op

	data, err := msgpack.Marshal(pkt.Data)

	if err != nil {
		log.Fatal(err)
	}

	data = append(header, data...) // Some Variadic Bullshit!!

	return data
}

func (pkt *Packet) Unmarshal(data []byte) {
	pkt.Id = binary.BigEndian.Uint64(data)
	pkt.Op = data[8]

	log.Printf("Unmarshling Packet Id %d and Op %d", pkt.Id, pkt.Op)

	payload := data[9:]

	var struc Payload

	switch pkt.Op {
	case AttrRequest:
		struc = &RemotePath{}
	case ReadDirRequest:
		struc = &RemotePath{}
	case FetchFileRequest:
		struc = &RemotePath{}
	case ReadFileRequest:
		struc = &ReadInfo{}
	case WriteFileRequest:
		struc = &WriteInfo{}
	case TruncateRequest:
		struc = &TruncInfo{}
	case StatResponse:
		struc = &Stat{}
	case StatsResponse:
		struc = &DirInfo{}
	case FileDataResponse:
		struc = &FileChunk{}
	case WriteResponse:
		struc = &WriteResult{}
	case EmptyResponse:
		struc = nil
	}

	if pkt.Op != EmptyResponse {

		err := msgpack.Unmarshal(payload, struc)
		if err != nil {
			log.Fatal(err)
		}

		pkt.Data = struc
	}

}
