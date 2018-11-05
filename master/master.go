package master

import (
	"fmt"
	"math/rand"
	"net"
	"simpledfs/membership"
	"simpledfs/utils"
	"time"
)

var meta utils.Meta

var hashtextToFilenameMap map[string]string

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
	hashtextToFilenameMap[utils.Hash2Text(pr.FilenameHash[:])] = filename
	pr.Filesize = prMsg.Filesize
	pr.Timestamp = uint64(timestamp)
	dnList, err := utils.HashReplicaRange(filename, uint32(mn.MemberList.Size()))
	utils.PrintError(err)
	for k, v := range dnList {
		m, err := mn.MemberList.RetrieveByIdx(int(v))
		if err != nil {
			utils.PrintError(err)
		} else {
			pr.DataNodeList[k] = utils.NodeID{Timestamp: m.Timestamp, IP: m.IP}
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

func (mn *masterNode) HandleGetVersionsRequest(gvrMsg utils.GetVersionsRequest, conn net.Conn) {
	filename := utils.ParseFilename(gvrMsg.Filename[:])
	fmt.Println("Get version filename ", filename)
	numVersions := gvrMsg.VersionNum

	filenameHash := utils.HashFilename(filename)
	infos, ok := meta.FileInfos(utils.Hash2Text(filenameHash[:]))
	if ok == false {
		gvr := utils.GetVersionsResponse{MsgType: utils.GetVersionsResponseMsg}
		gvr.VersionNum = 0
		bin := utils.Serialize(gvr)
		conn.Write(bin)
		return
	}

	if numVersions > 5 {
		numVersions = 5
	}

	if numVersions > uint8(len(infos)) {
		numVersions = uint8(len(infos))
	}

	for _, info := range infos[:numVersions] {
		gvr := utils.GetVersionsResponse{
			MsgType:    utils.GetVersionsResponseMsg,
			VersionNum: numVersions,
			Timestamp:  info.Timestamp,
		}
		fmt.Println("Handle GetVersionsRequest and respond:", info.Timestamp)
		gvr.FilenameHash = utils.HashFilename(filename)
		gvr.Filesize = info.Filesize
		nodeIPs := [utils.NumReplica]uint32{}
		nodePorts := [utils.NumReplica]uint16{}
		for k, v := range info.DataNodes {
			nodeIPs[k] = v.IP
			nodePorts[k] = mn.DNPort
		}
		gvr.DataNodeIPList = nodeIPs
		gvr.DataNodePortList = nodePorts

		bin := utils.Serialize(gvr)
		conn.Write(bin)
	}

}

func (mn *masterNode) HandleDeleteRequest(drMsg utils.DeleteRequest, conn net.Conn) {
	filename := utils.ParseFilename(drMsg.Filename[:])
	fmt.Println("filename ", filename)
	filenameHash := utils.HashFilename(filename)
	infos, ok := meta.RmFileInfo(utils.Hash2Text(filenameHash[:]))
	dr := utils.DeleteResponse{MsgType: utils.DeleteResponseMsg, IsSuccess: ok}
	bin := utils.Serialize(dr)
	conn.Write(bin)

	ids := make(map[utils.NodeID]int, 0)
	for _, info := range infos {
		nodes := info.DataNodes
		for _, node := range nodes {
			ids[node] = 1
		}
	}
	for id, _ := range ids {
		rr := utils.RmRequest{MsgType: utils.RmRequestMsg, FilenameHash: filenameHash}
		mn.RmRequest(rr, utils.StringIP(id.IP)+":"+utils.StringPort(mn.DNPort))
	}
}

func (mn *masterNode) RmRequest(rr utils.RmRequest, addr string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		utils.PrintError(err)
		fmt.Println("Failed to connect Delete datanode")
		return
	}

	conn.Write(utils.Serialize(rr))
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
		filename := hashtextToFilenameMap[val]
		buf := make([]byte, 128)
		copy(buf[:], filename)
		conn.Write(buf)
	}
	return
}

// Re-replica go routine for consistently check if a file has kept in four replica
// Send the re-replica request to a node who has this file and pipeline the checking
func (mn *masterNode) ReReplicaRoutine() {
	for {
		for file, infos := range meta {
			filename := hashtextToFilenameMap[file]
			for _, info := range infos {
				dataNodes := info.DataNodes
				rrr := utils.ReReplicaRequest{
					MsgType:      utils.ReReplicaRequestMsg,
					FilenameHash: utils.HashFilename(filename),
					Timestamp:    info.Timestamp,
					TimeToLive:   4,
				}

				ids := make([]utils.NodeID, 0)
				for _, id := range dataNodes {
					_, err := mn.MemberList.Retrieve(id.Timestamp, id.IP)
					if err != nil {
						utils.PrintError(err)
						continue
					}
					ids = append(ids, id)
				}

				if len(ids) < utils.NumReplica && mn.MemberList.Size() >= utils.NumReplica {
					picksID := mn.pickReceivers(ids, utils.NumReplica-len(ids))
					for i := 0; i < utils.NumReplica; i++ {
						if i < len(ids) {
							rrr.DataNodeList[i] = ids[i]
						} else {
							rrr.DataNodeList[i] = picksID[i-len(ids)]
						}
					}
				} else {
					continue
				}
				fmt.Println(rrr.DataNodeList)

				mn.ReReplicaRequest(rrr, utils.StringIP(ids[0].IP)+":"+utils.StringPort(mn.DNPort))
				meta.UpdateFileInfo(utils.Hash2Text(rrr.FilenameHash[:]), rrr.DataNodeList[:])
			}
		}
	}
}

func (mn *masterNode) ReReplicaRequest(rrr utils.ReReplicaRequest, addr string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		utils.PrintError(err)
		fmt.Println("Failed to connect Re-replica node")
		return
	}

	conn.Write(utils.Serialize(rrr))
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
	case utils.GetVersionsRequestMsg:
		gvr := utils.GetVersionsRequest{}
		utils.Deserialize(buf[:n], &gvr)
		mn.HandleGetVersionsRequest(gvr, conn)
	default:
		fmt.Println("Unrecognized packet")
	}
}

func (mn *masterNode) pickReceivers(fileHolders []utils.NodeID, num int) []utils.NodeID {
	receivers := make([]utils.NodeID, 0)
	candidates := make([]utils.NodeID, 0)

	// All nodes excluding file holders are candidates
	for i := 0; i < mn.MemberList.Size(); i++ {
		member := mn.MemberList.Members[i]
		isFileHolder := false
		for _, fileHolder := range fileHolders {
			if member.Timestamp == fileHolder.Timestamp && member.IP == fileHolder.IP {
				isFileHolder = true
				break
			}
		}
		if isFileHolder == false {
			candidates = append(candidates, utils.NodeID{member.Timestamp, member.IP})
		}
	}

	fmt.Println(candidates)

	if len(candidates) < num {
		for i := len(candidates); i < num; i++ {
			candidates = append(candidates, utils.NodeID{0, 0})
		}
		return candidates
	}

	receiverIndexs := rand.Perm(len(candidates))[:num] // Pick random receivers

	for _, recvIdx := range receiverIndexs {
		receivers = append(receivers, candidates[recvIdx])
	}

	return receivers
}

func (mn *masterNode) Start() {
	//meta = utils.NewMeta("MasterMeta")
	meta = utils.Meta{}
	hashtextToFilenameMap = make(map[string]string)

	listener, err := net.Listen("tcp", ":"+mn.Port)
	if err != nil {
		utils.PrintError(err)
		return
	}
	go mn.ReReplicaRoutine()

	for {
		conn, err := listener.Accept()
		defer conn.Close()
		if err != nil {
			// handle error
		}
		go mn.Handle(conn)
	}
}
