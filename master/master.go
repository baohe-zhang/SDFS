package main

import (
	"fmt"
	"net"
	"simpledfs/utils"
	"time"
)

type masterNode struct {
	Port string
}

func NewMasterNode(port string) *masterNode {
	mn := masterNode{Port: port}
	return &mn
}

func (mn *masterNode) HandlePutRequest(prMsg utils.PutRequest, conn net.Conn) {
	filename := utils.ParseFilename(prMsg.Filename[:])
	fmt.Println("filename ", filename)
	fmt.Println("filesize ", prMsg.Filesize)

	pr := utils.PutResponse{MsgType: utils.PutResponseMsg}
	pr.FilenameHash = utils.HashFilename(filename)
	fmt.Println(utils.Hash2Text(pr.FilenameHash[:]))
	pr.Filesize = prMsg.Filesize
	pr.Timestamp = uint64(time.Now().UnixNano())
	dnList, err := utils.HashReplicaRange(filename, 10)
	utils.PrintError(err)
	pr.DataNodeList = dnList
	pr.NexthopIP = utils.BinaryIP(utils.GetLocalIP().String())
	pr.NexthopPort = uint16(8000)

	bin := utils.Serialize(pr)
	conn.Write(bin)
}

func (mn *masterNode) HandleWriteConfirm(wcMsg utils.WriteConfirm, conn net.Conn) {

}

func (mn *masterNode) handle(conn net.Conn) {
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	fmt.Println(n)
	utils.PrintError(err)

	switch buf[0] {
	case utils.PutRequestMsg:
		pr := utils.PutRequest{}
		utils.Deserialize(buf[:n], &pr)
		mn.HandlePutRequest(pr, conn)
	case utils.WriteConfirmMsg:
		wc := utils.WriteConfirm{}
		utils.Deserialize(buf[:n], &wc)
		mn.HandleWriteConfirm(wc, conn)
	default:
		fmt.Println("Unrecognized packet")
	}
}

func (mn *masterNode) start() {
	listener, err := net.Listen("tcp", ":"+mn.Port)
	if err != nil {
		// handle error
	}
	for {
		conn, err := listener.Accept()
		defer conn.Close()
		if err != nil {
			// handle error
		}
		go mn.handle(conn)
	}
}

func main() {
	node := NewMasterNode("5000")
	node.start()
}
