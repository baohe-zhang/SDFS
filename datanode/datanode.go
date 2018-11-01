package main

import (
	"fmt"
	"net"
	"simpledfs/utils"
	// "time"
	// "sync"
	// "bytes"
	"errors"
	"io"
	"os"
)

var NodeID uint8
var meta utils.Meta

var MemberList = [...]string{
	"fa18-cs425-g29-01.cs.illinois.edu",
	"fa18-cs425-g29-02.cs.illinois.edu",
	"fa18-cs425-g29-03.cs.illinois.edu",
	"fa18-cs425-g29-04.cs.illinois.edu",
	"fa18-cs425-g29-05.cs.illinois.edu",
	"fa18-cs425-g29-06.cs.illinois.edu",
	"fa18-cs425-g29-07.cs.illinois.edu",
	"fa18-cs425-g29-08.cs.illinois.edu",
	"fa18-cs425-g29-09.cs.illinois.edu",
	"fa18-cs425-g29-10.cs.illinois.edu",
}

const (
	BufferSize = 4096
)

func listener() {
	ln, err := net.Listen("tcp", ":8000")
	if err != nil {
		fmt.Println(err.Error())
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err.Error())
		}
		go handler(conn)
	}
}

func handler(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, BufferSize)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println(err.Error())
	}

	if buf[0]&utils.WriteRequestMsg != 0 {
		// Receive write request from client
		msg := utils.WriteRequest{}
		utils.Deserialize(buf[:n], &msg)

		fileReader(conn, msg)

	} else if buf[0]&utils.ReadRequestMsg != 0 {
		// Receive read request from client
		msg := utils.ReadRequest{}
		utils.Deserialize(buf[:n], &msg)

		fileWriter(conn, msg)
	}
}

// Receive remote file from cleint, store it in local and send it to next hop if possible
func fileReader(conn net.Conn, wr utils.WriteRequest) {
	filesize := wr.Filesize
	// Create local filename from write request
	_filename := utils.Hash2Text(wr.FilenameHash[:])
	timestamp := fmt.Sprintf("%d", wr.Timestamp)
	filename := _filename + ":" + timestamp

	fmt.Println("filename: ", filename)

	// Create file descriptor
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file.Close()

	// Check whether next node exists
	hasNextNode := true
	nextNodeConn, err := dialDataNode(wr)
	if err != nil {
		fmt.Println(err.Error())
		hasNextNode = false
	} else {
		fmt.Println("next node addr: ", (*nextNodeConn).RemoteAddr().String())
	}

	// Ready to receive file
	conn.Write([]byte("OK"))

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
		meta.PutFileInfo(_filename, info)
		meta.StoreMeta("meta.json")
		fmt.Printf("put %s with ts %d into meta list\n", _filename, wr.Timestamp)

		// Tell master it receives a file
		// dialMasterNode()
	}
}

// Send local file to client
func fileWriter(conn net.Conn, rr utils.ReadRequest) {
	// Retrieve local filename from read request and meta data
	filename := utils.Hash2Text(rr.FilenameHash[:])
	info := meta.FileInfo(filename)
	timestamp := fmt.Sprintf("%d", info.Timestamp)
	filename = filename + ":" + timestamp

	// Send file to client
	file, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file.Close()

	// Block until it receives OK
	buf := make([]byte, BufferSize)
	n, _ := conn.Read(buf)
	for n > 0 {
		if string(buf[:n]) == "OK" {
			break
		} else {
			buf = make([]byte, BufferSize)
			n, _ = conn.Read(buf)
		}
	}

	fmt.Println("client ready to receive file")

	buf = make([]byte, BufferSize)

	for {
		n, err := file.Read(buf)
		conn.Write(buf[:n])
		if err == io.EOF {
			fmt.Printf("send file %s finish\n", filename)
			break
		}
	}
}

func dialMasterNode(masterID uint8, filenameHash [32]byte, filesize uint64, timestamp uint64) {
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
		DataNode:     NodeID,
	}

	conn.Write(utils.Serialize(wc))
}

func dialDataNode(wr utils.WriteRequest) (*net.Conn, error) {
	nodeID, err := getNexthopID(wr.DataNodeList[:])
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial("tcp", getNodeIP(nodeID)+":"+getNodePort(nodeID))
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
	fmt.Printf("node %d ready to receive file", nodeID)

	return &conn, nil
}

// Return the first non-zero nodeID's index
func getNexthopID(nodeList []uint8) (uint8, error) {
	for k, v := range nodeList {
		if v == NodeID && k < len(nodeList)-1 {
			return nodeList[k+1], nil
		}
	}
	return 255, errors.New("Nexthop doesn't exists")
}

func getNodeIP(nodeID uint8) string {
	return MemberList[nodeID]
}

func getNodePort(nodeID uint8) string {
	return "8000"
}

func getNodeID(hostname string) (uint8, error) {
	for k, v := range MemberList {
		if hostname == v {
			return uint8(k), nil
		}
	}
	return 255, errors.New("hostname doesn't match any nodeID")
}

func main() {
	var err error
	NodeID, err = getNodeID(utils.GetLocalHostname())
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Node ID: ", NodeID)
	}

	meta = utils.NewMeta("meta.json")

	go listener()

	select {}
}
