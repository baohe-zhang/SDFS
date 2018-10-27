package daemon

import ()

const (
	NumReplica = 4
)

type PutRequest struct {
	Filename [128]byte
	Filesize uint32
}

type PutResponse struct {
	Filename     [128]byte
	Filesize     uint32
	Timestamp    uint64
	NexthopIP    uint32
	NexthopPort  uint16
	DataNodeList [NumReplica]uint8
}

type PutAcknowledge struct {
}

type WriteRequest struct {
	Filename     [128]byte
	Filesize     uint32
	Timestamp    uint64
	DataNodeList [NumReplica]uint8
}

type WriteResponse struct {
	Filename  [128]byte
	Filesize  uint32
	Timestamp uint64
	DataNode  uint8
}












