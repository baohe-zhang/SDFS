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
	NodeID     = 1
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
	filename := utils.Hash2Text(wr.FilenameHash[:])
	timestamp := fmt.Sprintf("%d", wr.Timestamp)
	filename = filename + ":" + timestamp

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
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file.Close()

	buf := make([]byte, BufferSize)
	n, err := conn.Read(buf)

	for string(buf[:n]) != "OK" {}
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
	nodeID := findNexthop(wr.DataNodeList[:])
	if nodeID == -1 {
		return nil, errors.New("Empty DataNodeList")
	}

	conn, err := net.Dial("tcp", getNodeIP(wr.DataNodeList[nodeID])+":"+getNodePort(wr.DataNodeList[nodeID]))
	if err != nil {
		return &conn, err
	}
	// Clear nexthop in the node list
	wr.DataNodeList[nodeID] = 0

	// Send write request to the next hop
	conn.Write(utils.Serialize(wr))

	// Wait for next hop's reply
	buf := make([]byte, BufferSize)
	n, err := conn.Read(buf)
	for string(buf[:n]) != "OK" {}
	fmt.Printf("node %d ready to receive file", nodeID)

	return &conn, nil
}

// Return the first non-zero nodeID's index
func findNexthop(nodeList []uint8) int {
	for k, v := range nodeList {
		if v != 0 {
			return k
		}
	}
	return -1
}

func getNodeIP(nodeid uint8) string {
	return MemberList[nodeid]
}

func getNodePort(nodeid uint8) string {
	return "8000"
}

func main() {

	meta = utils.NewMeta("meta3.json")

	go listener()

	select {}
}
