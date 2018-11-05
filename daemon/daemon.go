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

func detectNodeFailure(elector *election.Elector, tch chan uint64, ich chan uint32) {
	for {
		select {
			case <-tch:
				ip := <-ich
				fmt.Printf("node %s failed\n", utils.StringIP(ip))
				if utils.StringIP(ip) == "172.22.156.95" {
					fmt.Printf("master failed. election start\n")
					elector.Election()
				}

			default:
		}
	}
}

func detectNewMaster(mch chan uint32) {
	for {
		select {
			case mip := <-mch:
				fmt.Printf("daemon has new master ip %s\n", utils.StringIP(mip))

			default:
		}
	}
}

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

	tsch := make(chan uint64) // channel to notify node failure
	ipch := make(chan uint32)
	msch := make(chan uint32) // channel to notify new master


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
	go elector.Start("5003", msch) 

	go detectNodeFailure(elector, tsch, ipch)
	go detectNewMaster(msch)

	membership.Start(masterIP, fmt.Sprintf("%d", membershipPort), tsch, ipch)
}
