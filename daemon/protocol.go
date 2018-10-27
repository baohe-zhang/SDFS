package daemon

import ()

const (
	NumReplica = 4
)

type PutRequest struct {
	Filename [128]byte
	Filesize uint64
}

type PutResponse struct {
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
	Filename     [128]byte
	Filesize     uint64
	Timestamp    uint64
	DataNodeList [NumReplica]uint8
}

type WriteConfirm struct {
	Filename  [128]byte
	Filesize  uint64
	Timestamp uint64
	DataNode  uint8
}












