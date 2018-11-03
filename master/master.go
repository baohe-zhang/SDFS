package master

import (
	"fmt"
	"net"
	"simpledfs/membership"
	"simpledfs/utils"
	"time"
)

var meta utils.Meta

var metaFile map[string]string

type masterNode struct {
	Port       string
	DNPort     uint16
	MemberList *membership.MemberList
}

func NewMasterNode(port string, dnPort uint16, memberList *membership.MemberList) *masterNode {
	mn := masterNode{Port: port, DNPort: dnPort, MemberList: memberList}
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
	metaFile[utils.Hash2Text(pr.FilenameHash[:])] = filename
	pr.Filesize = prMsg.Filesize
	pr.Timestamp = uint64(timestamp)
	dnList, err := utils.HashReplicaRange(filename, uint32(mn.MemberList.Size()))
	utils.PrintError(err)
	for k, v := range dnList {
		m, err := mn.MemberList.RetrieveByIdx(int(v))
		if err != nil {
			utils.PrintError(err)
		} else {
			pr.DataNodeList[k] = utils.NodeID{Timestamp: m.TimeStamp, IP: m.IP}
		}
	}

	pr.NexthopIP = pr.DataNodeList[0].IP
	pr.NexthopPort = mn.DNPort

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
	info, ok := meta.FileInfo(utils.Hash2Text(gr.FilenameHash[:]))
	gr.Filesize = info.Filesize
	if ok == false {
		gr.Filesize = 0
	}
	nodeIPs := [utils.NumReplica]uint32{}
	nodePorts := [utils.NumReplica]uint16{}
	for k, v := range info.DataNodes {
		nodeIPs[k] = v.IP
		nodePorts[k] = mn.DNPort
	}
	gr.DataNodeIPList = nodeIPs
	gr.DataNodePortList = nodePorts

	bin := utils.Serialize(gr)
	conn.Write(bin)

	return
}

func (mn *masterNode) HandleDeleteRequest(drMsg utils.DeleteRequest, conn net.Conn) {
	filename := utils.ParseFilename(drMsg.Filename[:])
	fmt.Println("filename ", filename)
	filenameHash := utils.HashFilename(filename)
	ok := meta.RmFileInfo(utils.Hash2Text(filenameHash[:]))
	dr := utils.DeleteResponse{MsgType: utils.DeleteResponseMsg, IsSuccess: ok}

	bin := utils.Serialize(dr)
	conn.Write(bin)
	return
}

func (mn *masterNode) HandleListRequest(lrMsg utils.ListRequest, conn net.Conn) {
	filename := utils.ParseFilename(lrMsg.Filename[:])
	fmt.Println("filename ", filename)
	filenameHash := utils.HashFilename(filename)
	info, ok := meta.FileInfo(utils.Hash2Text(filenameHash[:]))
	lr := utils.ListResponse{MsgType: utils.ListResponseMsg}
	var dnList [utils.NumReplica]uint32
	for index, value := range info.DataNodes {
		if ok == true {
			dnList[index] = value.IP
		} else {
			dnList[index] = 0
		}
	}

	lr.DataNodeIPList = dnList

	bin := utils.Serialize(lr)
	conn.Write(bin)
	return
}

func (mn *masterNode) HandleStoreRequest(srMsg utils.StoreRequest, conn net.Conn) {
	files := meta.FilesIn(utils.BinaryIP(conn.RemoteAddr().(*net.TCPAddr).IP.String()))
	sr := utils.StoreResponse{MsgType: utils.StoreResponseMsg, FilesNum: uint32(len(files))}

	bin := utils.Serialize(sr)
	conn.Write(bin)

	for _, val := range files {
		buf := make([]byte, 128)
		copy(buf[:], val)
		conn.Write(bin)
	}
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
	case utils.DeleteRequestMsg:
		dr := utils.DeleteRequest{}
		utils.Deserialize(buf[:n], &dr)
		mn.HandleDeleteRequest(dr, conn)
	case utils.ListRequestMsg:
		lr := utils.ListRequest{}
		utils.Deserialize(buf[:n], &lr)
		mn.HandleListRequest(lr, conn)
	case utils.StoreRequestMsg:
		sr := utils.StoreRequest{}
		utils.Deserialize(buf[:n], &sr)
		mn.HandleStoreRequest(sr, conn)
	default:
		fmt.Println("Unrecognized packet")
	}
}

func (mn *masterNode) Start() {
	//meta = utils.NewMeta("MasterMeta")
	meta = utils.Meta{}
	metaFile = make(map[string]string)

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
