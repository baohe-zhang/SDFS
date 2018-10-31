package main

import (
	"fmt"
	"net"
	"simpledfs/utils"
	"time"
)

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

var meta utils.Meta

type masterNode struct {
	Port string
}

func NewMasterNode(port string) *masterNode {
	mn := masterNode{Port: port}
	return &mn
}

func (mn *masterNode) HandlePutRequest(prMsg utils.PutRequest, conn net.Conn) {
	filename := utils.ParseFilename(prMsg.Filename[:])
	timestamp := time.Now().UnixNano()
	fmt.Println("filename: ", filename)
	fmt.Println("timestamp: ", timestamp)
	fmt.Println("filesize: ", prMsg.Filesize)

	pr := utils.PutResponse{MsgType: utils.PutResponseMsg}
	pr.FilenameHash = utils.HashFilename(filename)
	fmt.Println(utils.Hash2Text(pr.FilenameHash[:]))
	pr.Filesize = prMsg.Filesize
	pr.Timestamp = uint64(timestamp)
	dnList, err := utils.HashReplicaRange(filename, 10)
	utils.PrintError(err)
	pr.DataNodeList = dnList
	pr.NexthopIP = utils.BinaryIP(utils.LookupIP(MemberList[dnList[0]]))
	pr.NexthopPort = uint16(8000)

	bin := utils.Serialize(pr)
	conn.Write(bin)

	info := utils.Info{Timestamp: pr.Timestamp, Filesize: pr.Filesize, DataNodes: pr.DataNodeList[:]}
	meta.PutFileInfo(utils.Hash2Text(pr.FilenameHash[:]), info)
	return
}

func (mn *masterNode) HandleWriteConfirm(wcMsg utils.WriteConfirm, conn net.Conn) {

}

func (mn *masterNode) HandleGetRequest(grMsg utils.GetRequest, conn net.Conn) {
	filename := utils.ParseFilename(grMsg.Filename[:])
	fmt.Println("filename ", filename)

	gr := utils.GetResponse{MsgType: utils.GetResponseMsg}
	gr.FilenameHash = utils.HashFilename(filename)
	fmt.Println(utils.Hash2Text(gr.FilenameHash[:]))
	info := meta.FileInfo(utils.Hash2Text(gr.FilenameHash[:]))
	gr.Filesize = info.Filesize
	nodeIPs := [utils.NumReplica]uint32{}
	nodePorts := [utils.NumReplica]uint16{}
	for k, v := range info.DataNodes {
		nodeIPs[k] = utils.BinaryIP(utils.LookupIP(MemberList[v]))
		nodePorts[k] = 8000
	}
	gr.DataNodeIPList = nodeIPs
	gr.DataNodePortList = nodePorts

	bin := utils.Serialize(gr)
	conn.Write(bin)

	return
}

func (mn *masterNode) Handle(conn net.Conn) {
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
	case utils.GetRequestMsg:
		gr := utils.GetRequest{}
		utils.Deserialize(buf[:n], &gr)
		mn.HandleGetRequest(gr, conn)
	default:
		fmt.Println("Unrecognized packet")
	}
}

func (mn *masterNode) start() {
	//meta = utils.NewMeta("MasterMeta")
	meta = utils.Meta{}

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
		go mn.Handle(conn)
	}
}

func main() {
	node := NewMasterNode("5000")
	node.start()
}
