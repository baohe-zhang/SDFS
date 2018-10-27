package daemon

import ()

const (
	NumReplica      = 4
	PutRequestMsg   = 0x01
	PutResponseMsg  = 0x01 << 1
	PutConfirmMsg   = 0x01 << 2
	WriteRequestMsg = 0x01 << 3
	WriteConfirmMsg = 0x01 << 4
)

type PutRequest struct {
	MsgType  uint8
	Filename [128]byte
	Filesize uint64
}

type PutResponse struct {
	MsgType      uint8
	Filename     [128]byte
	Filesize     uint64
	Timestamp    uint64
	NexthopIP    uint32
	NexthopPort  uint16
	DataNodeList [NumReplica]uint8
}

type PutConfirm struct {
}

type WriteRequest struct {
	MsgType      uint8
	Filename     [128]byte
	Filesize     uint64
	Timestamp    uint64
	DataNodeList [NumReplica]uint8
}

type WriteConfirm struct {
	MsgType   uint8
	Filename  [128]byte
	Filesize  uint64
	Timestamp uint64
	DataNode  uint8
}
