package datanode

import (
	"fmt"
	"net"
	"simpledfs/membership"
	"simpledfs/utils"
	// "time"
	// "sync"
	// "bytes"
	"errors"
	"io"
	"os"
)

var meta utils.Meta

const (
	BufferSize = 4096
)

type dataNode struct {
	NodeID     utils.NodeID
	NodePort   string
	MemberList *membership.MemberList
}

func NewDataNode(port string, memberList *membership.MemberList, nodeID utils.NodeID) *dataNode {
	dn := dataNode{NodePort: port, MemberList: memberList, NodeID: nodeID}
	return &dn
}

func (dn *dataNode) Listener() {
	ln, err := net.Listen("tcp", ":"+dn.NodePort)
	if err != nil {
		fmt.Println(err.Error())
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err.Error())
		}
		go dn.Handler(conn)
	}
}

func (dn *dataNode) Handler(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, BufferSize)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println(err.Error())
	}

	if buf[0] == utils.WriteRequestMsg {
		// Receive write request from client
		msg := utils.WriteRequest{}
		utils.Deserialize(buf[:n], &msg)

		dn.fileReader(conn, msg)

	} else if buf[0] == utils.ReadRequestMsg {
		// Receive read request from client
		msg := utils.ReadRequest{}
		utils.Deserialize(buf[:n], &msg)

		dn.fileWriter(conn, msg)
	} else if buf[0] == utils.ReReplicaRequestMsg {
		// Receive re-replica request from master or peer
		fmt.Println("Receive re-replica request")
		msg := utils.ReReplicaRequest{}
		utils.Deserialize(buf[:n], &msg)

		dn.reReplicaStat(conn, msg)
	}

}

// Handle Re-Replica Request message from master or peer datanode
func (dn *dataNode) reReplicaStat(conn net.Conn, rrrMsg utils.ReReplicaRequest) {
	hashFilename := utils.Hash2Text(rrrMsg.FilenameHash[:])
	fmt.Println("ReplicaStat", hashFilename)
	_, ok := meta.FileInfoWithTs(hashFilename, rrrMsg.Timestamp)

	rrg := utils.ReReplicaGet{MsgType: utils.ReReplicaGetMsg, FilenameHash: rrrMsg.FilenameHash, Timestamp: rrrMsg.Timestamp}
	if ok {
		rrg.GetNeed = false
	} else {
		rrg.GetNeed = true
	}

	bin := utils.Serialize(rrg)
	_, err := conn.Write(bin)
	utils.PrintError(err)
	if ok {
		dn.dialDataNodeReReplica(rrrMsg)
		fmt.Println("There has been a replica existing")
		return
	}

	buf := make([]byte, 97)
	n, err := conn.Read(buf)
	utils.PrintError(err)

	response := utils.ReReplicaResponse{}
	utils.Deserialize(buf[:n], &response)
	if response.MsgType != utils.ReReplicaResponseMsg {
		fmt.Println("Unexpected message from DataNode")
		return
	}
	dn.reReplicaReader(conn, response)

	dn.dialDataNodeReReplica(rrrMsg)
	meta.UpdateFileInfo(hashFilename, rrrMsg.DataNodeList[:])
}

func (dn *dataNode) reReplicaReader(conn net.Conn, rrrMsg utils.ReReplicaResponse) {
	// Create local filename from re-replica
	filesize := rrrMsg.Filesize
	hashFilename := utils.Hash2Text(rrrMsg.FilenameHash[:])
	timestamp := fmt.Sprintf("%d", rrrMsg.Timestamp)
	filename := hashFilename + ":" + timestamp

	fmt.Println("Replica filename: ", filename)

	// Create file descriptor
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file.Close()

	// Ready to receive file
	conn.Write([]byte("OK"))
	fmt.Println("Sent OK")

	// Read file data from connection and write to local
	buf := make([]byte, BufferSize)
	var receivedBytes uint64

	for {
		n, err := conn.Read(buf)
		file.Write(buf[:n])
		receivedBytes += uint64(n)

		if err == io.EOF {
			fmt.Printf("Receive replica %s finish\n", filename)
			break
		}
	}

	// File size check
	if receivedBytes != filesize {
		fmt.Println("file size unmatch")
	} else {
		info := utils.Info{
			Timestamp: rrrMsg.Timestamp,
			Filesize:  rrrMsg.Filesize,
			DataNodes: rrrMsg.DataNodeList[:],
		}
		meta.PutFileInfo(hashFilename, info)
		fmt.Printf("put %s with ts %d into meta list\n", hashFilename, rrrMsg.Timestamp)
	}
}

func (dn *dataNode) reReplicaWriter(conn net.Conn, rrgMsg utils.ReReplicaGet) {
	hashFilename := utils.Hash2Text(rrgMsg.FilenameHash[:])
	info, ok := meta.FileInfoWithTs(hashFilename, rrgMsg.Timestamp)
	if ok == false {
		fmt.Println("ReReplica failed for this node has no info of this file")
		return
	}

	file, err := os.OpenFile(hashFilename+":"+fmt.Sprintf("%d", info.Timestamp), os.O_RDONLY, 0755)
	utils.PrintError(err)

	rrr := utils.ReReplicaResponse{
		MsgType:      utils.ReReplicaResponseMsg,
		Filesize:     info.Filesize,
		Timestamp:    info.Timestamp,
		FilenameHash: rrgMsg.FilenameHash,
	}

	for k, v := range info.DataNodes {
		rrr.DataNodeList[k] = v
	}

	bin := utils.Serialize(rrr)
	_, err = conn.Write(bin)
	utils.PrintError(err)

	buf := make([]byte, BufferSize)
	n, err := conn.Read(buf)
	for string(buf[:n]) != "OK" {
	}
	fmt.Println(string(buf[:n]))

	buf = make([]byte, BufferSize)
	for {
		n, err := file.Read(buf)
		conn.Write(buf[:n])
		if err == io.EOF {
			fmt.Println("Send ReReplica", hashFilename, "finish")
			break
		}
	}

}

// Receive remote file from cleint, store it in local and send it to next hop if possible
func (dn *dataNode) fileReader(conn net.Conn, wr utils.WriteRequest) {
	filesize := wr.Filesize
	// Create local filename from write request
	hashFilename := utils.Hash2Text(wr.FilenameHash[:])
	timestamp := fmt.Sprintf("%d", wr.Timestamp)
	filename := hashFilename + ":" + timestamp

	fmt.Println("filename: ", filename)

	// Create file descriptor
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file.Close()

	// Check whether next node exists
	hasNextNode := true
	nextNodeConn, err := dn.dialDataNode(wr)
	if err != nil {
		fmt.Println(err.Error())
		hasNextNode = false
	} else {
		fmt.Println("next node addr: ", (*nextNodeConn).RemoteAddr().String())
		defer (*nextNodeConn).Close()
	}

	// Ready to receive file
	conn.Write([]byte("OK"))
	fmt.Println("Sent OK")

	// Read file data from connection and write to local
	buf := make([]byte, BufferSize)
	var receivedBytes uint64
	for {
		n, err := conn.Read(buf)
		file.Write(buf[:n])
		receivedBytes += uint64(n)

		// Send file data to next node
		if hasNextNode {
			(*nextNodeConn).Write(buf[:n])
		}

		if err == io.EOF {
			fmt.Printf("receive file %s finish\n", filename)
			break
		}
	}

	// File size check
	if receivedBytes != filesize {
		fmt.Println("file size unmatch")
	} else {
		info := utils.Info{
			Timestamp: wr.Timestamp,
			Filesize:  wr.Filesize,
			DataNodes: wr.DataNodeList[:],
		}
		meta.PutFileInfo(hashFilename, info)
		meta.StoreMeta("meta.json")
		fmt.Printf("put %s with ts %d into meta list\n", hashFilename, wr.Timestamp)

		// Tell master it receives a file
		// dialMasterNode()
	}
}

// Send local file to client
func (dn *dataNode) fileWriter(conn net.Conn, rr utils.ReadRequest) {
	defer conn.Close()

	// Retrieve local filename from read request and meta data
	filename := utils.Hash2Text(rr.FilenameHash[:])
	info, ok := meta.FileInfo(filename)
	if ok == false {
		conn.Write([]byte(" "))
		fmt.Println("Local file requested not found")
		return
	}
	timestamp := fmt.Sprintf("%d", info.Timestamp)
	filename = filename + ":" + timestamp

	// Send file to client
	file, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file.Close()

	// Block until it receives OK
	/*buf := make([]byte, BufferSize)*/
	//n, _ := conn.Read(buf)
	//for n > 0 {
	//if string(buf[:n]) == "OK" {
	//break
	//} else {
	//buf = make([]byte, BufferSize)
	//n, _ = conn.Read(buf)
	//}
	/*}*/

	fmt.Println("client ready to receive file")

	buf := make([]byte, BufferSize)

	for {
		n, err := file.Read(buf)
		conn.Write(buf[:n])
		if err == io.EOF {
			fmt.Printf("send file %s finish\n", filename)
			break
		}
	}

}

func (dn *dataNode) dialMasterNode(masterID uint8, filenameHash [32]byte, filesize uint64, timestamp uint64) {
	conn, err := net.Dial("tcp", ":8000")
	if err != nil {
		fmt.Println(err.Error())
	}
	defer conn.Close()

	wc := utils.WriteConfirm{
		MsgType:      utils.WriteConfirmMsg,
		FilenameHash: filenameHash,
		Filesize:     filesize,
		Timestamp:    timestamp,
		DataNode:     dn.NodeID,
	}

	conn.Write(utils.Serialize(wc))
}

// Dial DataNode with WriteRequest
func (dn *dataNode) dialDataNode(wr utils.WriteRequest) (*net.Conn, error) {
	nodeID, err := dn.getNexthopID(wr.DataNodeList[:])
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial("tcp", utils.StringIP(nodeID.IP)+":"+dn.NodePort)
	if err != nil {
		return nil, err
	}

	// Send write request to the next hop
	conn.Write(utils.Serialize(wr))

	// Wait for next hop's reply
	buf := make([]byte, BufferSize)
	n, err := conn.Read(buf)
	for string(buf[:n]) != "OK" {
	}
	fmt.Printf("node %v ready to receive file", nodeID)

	return &conn, nil
}

// Dial DataNode with ReReplicaRequest transfer
func (dn *dataNode) dialDataNodeReReplica(rrr utils.ReReplicaRequest) {
	rrr.TimeToLive -= 1
	if rrr.TimeToLive == 0 {
		return
	}

	nodeID, err := dn.getNexthopIDCircle(rrr.DataNodeList[:])
	if err != nil {
		fmt.Println("Get Node ID failed")
		return
	}

	conn, err := net.Dial("tcp", utils.StringIP(nodeID.IP)+":"+dn.NodePort)
	if err != nil {
		utils.PrintError(err)
		return
	}

	// Send re-replica request to the next hop
	conn.Write(utils.Serialize(rrr))
	fmt.Println("Dial the next replica node")

	buf := make([]byte, 42)
	n, err := conn.Read(buf)
	utils.PrintError(err)

	response := utils.ReReplicaGet{}
	utils.Deserialize(buf[:n], &response)
	if response.MsgType != utils.ReReplicaGetMsg {
		fmt.Println("Unexpected message from DataNode")
		return
	}

	if response.GetNeed == false {
		return
	}

	dn.reReplicaWriter(conn, response)
}

// Return the first non-zero nodeID's index
func (dn *dataNode) getNexthopID(nodeList []utils.NodeID) (utils.NodeID, error) {
	for k, v := range nodeList {
		if v == dn.NodeID && k < len(nodeList)-1 &&
			nodeList[k+1].IP != 0 && nodeList[k+1].Timestamp != 0 {
			return nodeList[k+1], nil
		}
	}
	return utils.NodeID{}, errors.New("Nexthop doesn't exists")
}

// Return the first non-zero nodeID's index
func (dn *dataNode) getNexthopIDCircle(nodeList []utils.NodeID) (utils.NodeID, error) {
	for k, v := range nodeList {
		if v == dn.NodeID && k < len(nodeList) &&
			nodeList[(k+1)%len(nodeList)].IP != 0 && nodeList[(k+1)%len(nodeList)].Timestamp != 0 {
			return nodeList[(k+1)%len(nodeList)], nil
		}
	}
	return utils.NodeID{}, errors.New("Nexthop doesn't exists")
}

func (dn *dataNode) Start() {
	meta = utils.NewMeta("meta.json")

	dn.Listener()
}
