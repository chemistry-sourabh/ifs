package arsyncfs

import (
	"github.com/vmihailenco/msgpack"
	"log"
	"encoding/binary"
)

type Payload interface {
}

// TODO Add Error in the Structure
type Packet struct {
	Id uint64 // TODO What if this overflows ?
	Op uint8
	Data Payload
}

func (pkt *Packet) Marshal() ([]byte, error) {
	header := make([]byte, 9)
	binary.BigEndian.PutUint64(header, pkt.Id)
	header[8] = pkt.Op

	data, err := msgpack.Marshal(pkt.Data)

	if err != nil {
		return nil, err
	}

	data = append(header, data...) // Some Variadic Bullshit!!

	return data, nil
}

func (pkt *Packet) Unmarshal(data []byte) {
	pkt.Id = binary.BigEndian.Uint64(data)
	pkt.Op = data[8]

	log.Printf("Unmarshling Packet Id %d and Op %s", pkt.Id, ConvertOpCodeToString(pkt.Op))

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
	case CreateRequest:
		struc = &CreateInfo{}
	case RemoveRequest:
		struc = &RemotePath{}
	case StatResponse:
		struc = &Stat{}
	case StatsResponse:
		struc = &DirInfo{}
	case FileDataResponse:
		struc = &FileChunk{}
	case WriteResponse:
		struc = &WriteResult{}
	case ErrorResponse:
		var payloadError error
		struc = &payloadError
	}

	err := msgpack.Unmarshal(payload, struc)
	if err != nil {
		log.Fatal(err)
	}

	if pkt.Op != ErrorResponse {
		pkt.Data = struc
	} else {
		pkt.Data = *struc.(*error)
	}

}
