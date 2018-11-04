package main

import (
	"flag"
	"fmt"
	"simpledfs/datanode"
	"simpledfs/master"
	"simpledfs/membership"
	"simpledfs/utils"
	"simpledfs/election"
)

func main() {
	masterIpPtr := flag.String("master", "127.0.0.1", "Master's IP")
	masternodePortPtr := flag.Int("mn-port", 5000, "MasterNode serving port")
	membershipPortPtr := flag.Int("mem-port", 5001, "Membership serving port")
	datanodePortPtr := flag.Int("dn-port", 5002, "DataNode serving port")
	flag.Parse()
	masterIP := *masterIpPtr
	masternodePort := *masternodePortPtr
	membershipPort := *membershipPortPtr
	datanodePort := *datanodePortPtr
	masterIP = utils.LookupIP(masterIP)
	localIP := utils.GetLocalIP().String()

	if membership.Initilize() == true {
		fmt.Printf("[INFO]: Start service\n")
	}

	if masterIP == localIP {
		masterNode := master.NewMasterNode(fmt.Sprintf("%d", masternodePort), uint16(datanodePort), membership.MyList)
		go masterNode.Start()
	}
	nodeID := utils.NodeID{Timestamp: membership.MyMember.Timestamp, IP: membership.MyMember.IP}
	node := datanode.NewDataNode(fmt.Sprintf("%d", datanodePort), membership.MyList, nodeID)
	go node.Start()

	elector := election.NewElector(nodeID, membership.MyList)
	go elector.Start("5003")

	membership.Start(masterIP, fmt.Sprintf("%d", membershipPort))
}
