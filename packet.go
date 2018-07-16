package ifs

import (
	"github.com/vmihailenco/msgpack"
	"encoding/binary"
	"fmt"
	"go.uber.org/zap"
)

type Payload interface {
}

type Packet struct {
	ConnId uint8
	Flags  uint8
	Id     uint64 // TODO What if this overflows ?
	Op     uint8
	Data   Payload
}

func (pkt *Packet) Marshal() ([]byte, error) {
	header := make([]byte, 11)
	binary.BigEndian.PutUint64(header, pkt.Id)
	header[8] = pkt.Op
	header[9] = pkt.ConnId
	header[10] = pkt.Flags

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
	pkt.ConnId = data[9]
	pkt.Flags = data[10]

	payload := data[11:]

	var struc Payload

	switch pkt.Op {
	case AttrRequest:
		struc = &RemotePath{}
	case ReadDirRequest:
		struc = &ReadDirInfo{}
	case ReadDirAllRequest:
		struc = &RemotePath{}
	case FetchFileRequest:
		struc = &RemotePath{}
	case ReadFileRequest:
		struc = &ReadInfo{}
	case WriteFileRequest:
		struc = &WriteInfo{}
	case SetAttrRequest:
		struc = &AttrInfo{}
	case CreateRequest:
		struc = &CreateInfo{}
	case RemoveRequest:
		struc = &RemotePath{}
	case RenameRequest:
		struc = &RenameInfo{}
	case OpenRequest:
		struc = &OpenInfo{}
	case CloseRequest:
		struc = &CloseInfo{}
	case WatchDirRequest:
		struc = &WatchInfo{}

	case AttrUpdateRequest:
		struc = &AttrUpdateInfo{}

	case StatResponse:
		struc = &Stat{}
	case StatsResponse:
		struc = &DirInfo{}
	case FileDataResponse:
		struc = &FileChunk{}
	case WriteResponse:
		struc = &WriteResult{}
	case ErrorResponse:
		struc = &Error{}
	}

	err := msgpack.Unmarshal(payload, struc)
	if err != nil {
		zap.L().Fatal("Unmarshalling Packet Failed",
			zap.Error(err),
		)
	}

	pkt.Data = struc

}

func (pkt *Packet) String() string {
	return fmt.Sprintf("Id = %d Op = %s Data = %s", pkt.Id, ConvertOpCodeToString(pkt.Op), pkt.Data)
}

func (pkt *Packet) IsRequest() bool {
	if pkt.Flags == 0 {
		return true
	}

	return false
}
