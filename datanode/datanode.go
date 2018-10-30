package main

import (
	"fmt"
	"net"
	"simpledfs/utils"
	// "time"
	"sync"
	// "bytes"
	"errors"
	"io"
	"os"
)

var wg sync.WaitGroup

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
	wg.Done()

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
		// Receive write request
		msg := utils.WriteRequest{}
		utils.Deserialize(buf[:n], &msg)

		// Create file io
		fileReader(conn, msg)
	}
}

func fileReader(conn net.Conn, wr utils.WriteRequest) {
	filesize := wr.Filesize
	filename := utils.Hash2Text(wr.FilenameHash[:])

	var hasLocalFile bool
	var file1 *os.File
	hasLocalFile = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		fmt.Println("File does not exist and will be created")
		hasLocalFile = false
		// Create file io
		file1, err = os.Create(filename)
		utils.PrintError(err)
	}
	defer file1.Close()

	var hasNextNode bool
	hasNextNode = true
	connNextNode, err := dialDataNodePut(wr)
	if err != nil {
		utils.PrintError(err)
		hasNextNode = false
	}

	// Ready to receive file
	conn.Write([]byte("OK"))

	buf := make([]byte, BufferSize)
	var receivedBytes uint64

	for {
		n, err := conn.Read(buf)
		if !hasLocalFile {
			file1.Write(buf[:n])
		}
		if hasNextNode {
			(*connNextNode).Write(buf[:n])
		}
		receivedBytes += uint64(n)
		if err == io.EOF {
			fmt.Printf("receive file with %d bytes\n", receivedBytes)
			break
		}
	}

	if filesize != receivedBytes {
		fmt.Println("Unmatched two files")
	}
}

func fileSender() {
	conn, err := net.Dial("tcp", ":8000")
	if err != nil {
		fmt.Println(err.Error())
	}
	defer conn.Close()

	filename := "file.mov"
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file.Close()

	buf := make([]byte, BufferSize)
	n, err := conn.Read(buf)
	
	for string(buf[:n]) != "OK" {}
	fmt.Println(string(buf[:n]))

	buf = make([]byte, BufferSize)

	for {
		n, err := file.Read(buf)
		conn.Write(buf[:n])
		if err == io.EOF {
			fmt.Printf("send file finish\n")
			break
		}
	}
}

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

func dialDataNodePut(wr utils.WriteRequest) (*net.Conn, error) {
	nextIndex := findNexthop(wr.DataNodeList[:])
	if nextIndex == -1 {
		return nil, errors.New("Empty DataNodeList")
	}

	conn, err := net.Dial("tcp", getNodeIP(wr.DataNodeList[nextIndex])+":"+getNodePort(wr.DataNodeList[nextIndex]))
	if err != nil {
		return &conn, err
	}
	wr.DataNodeList[nextIndex] = 0

	conn.Write(utils.Serialize(wr))

	buf := make([]byte, BufferSize)
	n, err := conn.Read(buf)
	for string(buf[:n]) != "OK" {
	}
	fmt.Println(string(buf[:n]))

	return &conn, nil
}

func main() {

	wg.Add(1)
	go listener()

	wg.Wait()
	//fileSender()

	select {}
}
