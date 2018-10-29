package main

import (
	"fmt"
	"net"
	"simpledfs/utils"
	// "time"
	"sync"
	// "bytes"
	"io"
	"os"
)

var wg sync.WaitGroup

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
		//go fileReader(conn)
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

		filenameHash := msg.FilenameHash
		filesize := msg.Filesize
		//timestamp := msg.Timestamp
		//dataNodeList := msg.DataNodeList

		// Create file io
		//filename := fmt.Sprintf("%x", filenameHash)
		filename := utils.Hash2Text(filenameHash[:])
		fileReader(conn, filename, filesize)
	}
}

func fileReader(conn net.Conn, filename string, filesize uint64) {
	// Create file io
	file1, err := os.Create(filename)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file1.Close()

	// Create file io
	file2, err := os.Create(filename + "_copy")
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file2.Close()

	// Ready to receive file
	conn.Write([]byte("OK"))

	buf := make([]byte, BufferSize)
	var receivedBytes uint64

	for {
		n, err := conn.Read(buf)
		file1.Write(buf[:n])
		file2.Write(buf[:n])
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
			fmt.Printf("send file finish\n")
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

func dialDataNode(datanodeID uint8, filenameHash [32]byte, filesize uint64, timestamp uint64, dataNodeList [utils.NumReplica]uint8) {
	conn, err := net.Dial("tcp", ":8000")
	if err != nil {
		fmt.Println(err.Error())
	}

	wr := utils.WriteRequest{
		MsgType:      utils.WriteRequestMsg,
		FilenameHash: filenameHash,
		Filesize:     filesize,
		Timestamp:    timestamp,
		DataNodeList: dataNodeList,
	}

	conn.Write(utils.Serialize(wr))
}

func main() {

	wg.Add(1)
	go listener()

	wg.Wait()
	//fileSender()

	select {}
}
