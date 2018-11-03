package utils

import ()

const (
	NumReplica        = 4
	PutRequestMsg     = 1
	PutResponseMsg    = 2
	PutConfirmMsg     = 3
	WriteRequestMsg   = 4
	WriteConfirmMsg   = 5
	GetRequestMsg     = 6
	GetResponseMsg    = 7
	ReadRequestMsg    = 8
	DeleteRequestMsg  = 9
	DeleteResponseMsg = 10
	ListRequestMsg    = 11
	ListResponseMsg   = 12
	StoreRequestMsg   = 13
	StoreResponseMsg  = 14
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
	DataNodeList [NumReplica]NodeID
}

type PutConfirm struct {
}

type WriteRequest struct {
	MsgType      uint8
	FilenameHash [32]byte
	Filesize     uint64
	Timestamp    uint64
	DataNodeList [NumReplica]NodeID
}

type WriteConfirm struct {
	MsgType      uint8
	FilenameHash [32]byte
	Filesize     uint64
	Timestamp    uint64
	DataNode     NodeID
}

type GetRequest struct {
	MsgType  uint8
	Filename [128]byte
}

type GetResponse struct {
	MsgType          uint8
	FilenameHash     [32]byte
	Filesize         uint64
	DataNodeIPList   [NumReplica]uint32
	DataNodePortList [NumReplica]uint16
}

type ReadRequest struct {
	MsgType      uint8
	FilenameHash [32]byte
}

type DeleteRequest struct {
	MsgType  uint8
	Filename [128]byte
}

type DeleteResponse struct {
	MsgType   uint8
	IsSuccess bool
}

type ListRequest struct {
	MsgType  uint8
	Filename [128]byte
}

type ListResponse struct {
	MsgType        uint8
	DataNodeIPList [NumReplica]uint32
}

type StoreRequest struct {
	MsgType uint8
}

type StoreResponse struct {
	MsgType  uint8
	FilesNum uint32
}

type NodeID struct {
	Timestamp uint64
	IP        uint32
}
