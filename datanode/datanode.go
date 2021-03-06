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
	"path/filepath"
)

var meta utils.Meta
var masterIP string

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
		fmt.Println("Receive write request")
		msg := utils.WriteRequest{}
		utils.Deserialize(buf[:n], &msg)

		dn.fileReader(conn, msg)

	} else if buf[0] == utils.ReadRequestMsg {
		// Receive read request from client
		fmt.Println("Receive read request")
		msg := utils.ReadRequest{}
		utils.Deserialize(buf[:n], &msg)

		dn.fileWriter(conn, msg)
	} else if buf[0] == utils.ReReplicaRequestMsg {
		// Receive re-replica request from master or peer
		fmt.Println("Receive re-replica request")
		msg := utils.ReReplicaRequest{}
		utils.Deserialize(buf[:n], &msg)

		dn.reReplicaStat(conn, msg)
	} else if buf[0] == utils.ReadVersionRequestMsg {
		// Receive read version request from client
		fmt.Println("Receive read version request")
		msg := utils.ReadVersionRequest{}
		utils.Deserialize(buf[:n], &msg)

		dn.fileVersionWriter(conn, msg)
	} else if buf[0] == utils.RmRequestMsg {
		// Receive delete request from client
		fmt.Println("Receive delete request")
		msg := utils.RmRequest{}
		utils.Deserialize(buf[:n], &msg)

		dn.fileDeletor(conn, msg)
	}

}

// Handle Remove Request message from master
func (dn *dataNode) fileDeletor(conn net.Conn, rrMsg utils.RmRequest) {
	hashFilename := utils.Hash2Text(rrMsg.FilenameHash[:])
	_, ok := meta.RmFileInfo(hashFilename)
	if ok {
		// remove replica all its versions when rejoin
		files, err := filepath.Glob("./" + hashFilename + ":*")
		if err != nil {
			utils.PrintError(err)
		}
		for _, f := range files {
			if err := os.Remove(f); err != nil {
				utils.PrintError(err)
			}
		}
	}
}

// Handle Re-Replica Request message from master or peer datanode
func (dn *dataNode) reReplicaStat(conn net.Conn, rrrMsg utils.ReReplicaRequest) {
	hashFilename := utils.Hash2Text(rrrMsg.FilenameHash[:])
	fmt.Println("ReplicaStat", hashFilename)
	_, ok := meta.FileInfoWithTs(hashFilename, rrrMsg.Timestamp)
	meta.UpdateFileInfoWithTs(hashFilename, rrrMsg.DataNodeList[:], rrrMsg.Timestamp)

	rrg := utils.ReReplicaGet{MsgType: utils.ReReplicaGetMsg, FilenameHash: rrrMsg.FilenameHash, Timestamp: rrrMsg.Timestamp}
	if ok {
		rrg.GetNeed = false
	} else {
		rrg.GetNeed = true
	}
	bin := utils.Serialize(rrg)
	_, err := conn.Write(bin)
	utils.PrintError(err)

	if ok == true {
		fmt.Println("There has been a replica existing")
	} else {
		buf := make([]byte, 97)
		n, err := conn.Read(buf)
		utils.PrintError(err)

		response := utils.ReReplicaResponse{}
		utils.Deserialize(buf[:n], &response)
		if response.MsgType != utils.ReReplicaResponseMsg {
			fmt.Println("Unexpected message from DataNode", response.MsgType)
			return
		}
		dn.reReplicaReader(conn, response)
	}

	go dn.dialDataNodeReReplica(rrrMsg)
}

func (dn *dataNode) reReplicaReader(conn net.Conn, rrrMsg utils.ReReplicaResponse) {
	// Create local filename from re-replica
	filesize := rrrMsg.Filesize
	hashFilename := utils.Hash2Text(rrrMsg.FilenameHash[:])
	timestamp := fmt.Sprintf("%d", rrrMsg.Timestamp)
	filename := hashFilename + ":" + timestamp

	fmt.Println("Receiving replica filename: ", filename)

	// Create file descriptor
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file.Close()

	// Read file data from connection and write to local
	buf := make([]byte, BufferSize)
	var receivedBytes uint64

	for {
		n, err := conn.Read(buf)
		file.Write(buf[:n])
		receivedBytes += uint64(n)

		if err == io.EOF {
			fmt.Printf("Received replica %s\n", filename)
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
		//meta.StoreMeta("meta.json")
		fmt.Printf("put %s with ts %d into meta list\n", hashFilename, wr.Timestamp)

		// Tell master it receives a file
		dn.dialMasterNode(wr.FilenameHash, wr.Filesize, wr.Timestamp)
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

	//fmt.Println("client ready to receive file")

	buf := make([]byte, BufferSize)

	for {
		n, err := file.Read(buf)
		conn.Write(buf[:n])
		if err == io.EOF {
			fmt.Printf("Send file %s finish\n", filename)
			break
		}
	}

}

// Send local file with certain version to client
func (dn *dataNode) fileVersionWriter(conn net.Conn, rvr utils.ReadVersionRequest) {
	defer conn.Close()

	// Retrieve local filename from read request and meta data
	filename := utils.Hash2Text(rvr.FilenameHash[:])
	info, ok := meta.FileInfoWithTs(filename, rvr.Timestamp)
	fmt.Println("Fileinfo", info)

	if ok == false {
		conn.Write([]byte(" "))
		fmt.Println("Local file requested not found")
		return
	}
	timestamp := fmt.Sprintf("%d", rvr.Timestamp)

	// Send file to client
	file, err := os.OpenFile(filename+":"+timestamp, os.O_RDONLY, 0755)
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

	//fmt.Println("client ready to receive file")

	buf := make([]byte, BufferSize)

	for {
		n, err := file.Read(buf)
		conn.Write(buf[:n])
		if err == io.EOF {
			fmt.Printf("Send file %s finish\n", filename)
			break
		}
	}

}

func (dn *dataNode) dialMasterNode(filenameHash [32]byte, filesize uint64, timestamp uint64) {
	conn, err := net.Dial("tcp", masterIP + ":" + "5000")
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
	nid, err := dn.getNexthopID(rrr.DataNodeList[:])
	if err != nil {
		fmt.Println("Get Node ID failed")
		return
	}

	conn, err := net.Dial("tcp", utils.StringIP(nid.IP)+":"+dn.NodePort)
	if err != nil {
		utils.PrintError(err)
		return
	}

	// Send re-replica request to the next hop
	conn.Write(utils.Serialize(rrr))
	fmt.Println("Dial the next replica node", utils.StringIP(nid.IP))

	buf := make([]byte, BufferSize)
	n, err := conn.Read(buf)
	utils.PrintError(err)

	response := utils.ReReplicaGet{}
	utils.Deserialize(buf[:n], &response)
	if response.MsgType != utils.ReReplicaGetMsg {
		fmt.Println("Dial: Unexpected message from DataNode:", response.MsgType)
		return
	}

	if response.GetNeed == false {
		return
	}

	dn.reReplicaWriter(conn, response)
}

// Return the first nodeID
func (dn *dataNode) getNexthopID(nodeList []utils.NodeID) (utils.NodeID, error) {
	for k, v := range nodeList {
		if v == dn.NodeID && k < len(nodeList)-1 &&
			nodeList[k+1].IP != 0 && nodeList[k+1].Timestamp != 0 {
			return nodeList[k+1], nil
		}
	}
	return utils.NodeID{}, errors.New("Nexthop doesn't exists")
}

// Return the matched nodeID
func (dn *dataNode) getNexthopIDCircle(nodeList []utils.NodeID) (utils.NodeID, error) {
	for k, v := range nodeList {
		if v.IP == 0 || v.Timestamp == 0 {
			break
		}
		if v == dn.NodeID && k < len(nodeList) &&
			nodeList[(k+1)%len(nodeList)].IP != 0 && nodeList[(k+1)%len(nodeList)].Timestamp != 0 {
			return nodeList[(k+1)%len(nodeList)], nil
		}
	}
	return utils.NodeID{}, errors.New("Nexthop doesn't exists")
}

func detectMasterIP(dch chan string) {
	for {
		select {

		case mip := <-dch:
			masterIP = mip
			fmt.Printf("master IP change to %s\n", masterIP)

		default:
		}
	}
}

func (dn *dataNode) Start(msip string, dch chan string) {
	masterIP = msip
	meta = utils.NewMeta("meta.json")

	go detectMasterIP(dch)

	dn.Listener()
}
