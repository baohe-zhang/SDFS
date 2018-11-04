package election

import (
	"fmt"
	"net"
	"time"
	"simpledfs/membership"
	"simpledfs/utils"
)

const (
	Election          = 1
	OK                = 2
	Coordinator       = 3
	ElecTimeoutPeriod = 3000 * time.Millisecond
)

var ElectionPort string
var elecTimer *time.Timer

type Elector struct {
	NodeID     utils.NodeID
	MemberList *membership.MemberList
}

func NewElector(nodeid utils.NodeID, memberlist *membership.MemberList) *Elector {
	elector := Elector{
		NodeID: nodeid,
		MemberList: memberlist,
	}
	return &elector
}

func sendUDP(addr string, packet []byte) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer conn.Close()

	conn.Write(packet)
}

func (e *Elector) listener() {
	udpAddr, _ := net.ResolveUDPAddr("udp4", ElectionPort)
	uconn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer uconn.Close()

	for {
		packet := make([]byte, 256)
		_, addr, err := uconn.ReadFromUDP(packet)
		if err != nil {
			fmt.Println(err.Error())
		}

		e.handler(packet, addr)
	}
}

func (e *Elector) handler(packet []byte, addr *net.UDPAddr) {
	if packet[0] == Election {
		if e.NodeID.IP > utils.BinaryIP(addr.IP.String()) {
			sendUDP(addr.IP.String() + ":" + ElectionPort, []byte{OK})
			e.election()
		}

	} else if packet[0] == OK {
		stop := elecTimer.Stop()
		if stop {
			fmt.Printf("%s has higher IP, %s's election stops", addr.IP.String(), utils.StringIP(e.NodeID.IP))
		}

	} else if packet[0] == Coordinator {
		fmt.Printf("%s becomes new master", addr.IP.String())

	} else {
		fmt.Println("[electon] unknown packet")
	}
}

// Initiate an election when the node detects the master failed
func (e *Elector) election() {
	for i := 0; i < e.MemberList.Size(); i++ {
		member := e.MemberList.Members[i]
		if e.NodeID.IP < member.IP {
			sendUDP(utils.StringIP(member.IP) + ":" + ElectionPort, []byte{Election})
		}
	}

	// Set a timer, if not OK response within timeout, the election sender becomes master
	elecTimer = time.NewTimer(ElecTimeoutPeriod)
	go func() {
		<-elecTimer.C
		fmt.Printf("%s elected as the master", utils.StringIP(e.NodeID.IP))
		e.coordination()
	}()
}

func (e *Elector) coordination() {
	for i := 0; i < e.MemberList.Size(); i++ {
		member := e.MemberList.Members[i]
		sendUDP(utils.StringIP(member.IP) + ":" + ElectionPort, []byte{Coordinator})
	}
}

func (e *Elector) Start(port string) {
	ElectionPort = port
	fmt.Println("elector start")
	e.listener()
}

