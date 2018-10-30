package utils

import ()

const (
	NumReplica      = 4
	PutRequestMsg   = 1
	PutResponseMsg  = 2
	PutConfirmMsg   = 3
	WriteRequestMsg = 4
	WriteConfirmMsg = 5
	GetRequestMsg   = 6
	GetResponseMsg  = 7
	ReadRequestMsg  = 8
)

type PutRequest struct {
	MsgType  uint8
	Filename [128]byte
	Filesize uint64
}

type PutResponse struct {
	MsgType      uint8
	FilenameHash [32]byte
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
	FilenameHash [32]byte
	Filesize     uint64
	Timestamp    uint64
	DataNodeList [NumReplica]uint8
}

type WriteConfirm struct {
	MsgType      uint8
	FilenameHash [32]byte
	Filesize     uint64
	Timestamp    uint64
	DataNode     uint8
}

type GetRequest struct {
	MsgType  uint8
	Filename [128]byte
}

type GetResponse struct {
	MsgType          uint8
	FilenameHash     [32]byte
	Filesize         uint64
	NodeNum          uint8
	DataNodeIPList   [NumReplica]uint32
	DataNodePortList [NumReplica]uint16
}

type ReadRequest struct {
	MsgType      uint8
	FilenameHash [32]byte
}
