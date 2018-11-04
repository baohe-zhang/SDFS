package membership

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

const (
	Ping               = 0x01
	Ack                = 0x01 << 1
	MemInitRequest     = 0x01 << 2
	MemInitReply       = 0x01 << 3
	MemUpdateSuspect   = 0x01 << 4
	MemUpdateResume    = 0x01 << 5
	MemUpdateLeave     = 0x01 << 6
	MemUpdateJoin      = 0x01 << 7
	StateAlive         = 0x01
	StateSuspect       = 0x01 << 1
	StateMonit         = 0x01 << 2
	StateIntro         = 0x01 << 3
	InitTimeoutPeriod  = 2000 * time.Millisecond
	PingTimeoutPeriod  = 2000 * time.Millisecond
	PingSendingPeriod  = 100 * time.Millisecond
	SuspectPeriod      = 2000 * time.Millisecond
	UpdateDeletePeriod = 15000 * time.Millisecond
	LeaveDelayPeriod   = 2000 * time.Millisecond
	TimeToLive         = 4
	HeaderLength       = 4
)

type Header struct {
	Type     uint8
	Seq      uint16
	Reserved uint8
}

type Update struct {
	UpdateID        uint64
	TTL             uint8
	UpdateType      uint8
	MemberTimestamp uint64
	MemberIP        uint32
	MemberState     uint8
}

var initTimer *time.Timer
var SuspectTimerMap map[uint32]*time.Timer
var FailureTimerMap map[[2]uint64]*time.Timer
var MyMember *Member
var MyList *MemberList
var MyIP string

var IntroducerIP string
var MembershipPort string

var DuplicateUpdateMap map[uint64]bool
var UpdateCacheList *TtlCache
var Logger *ssmsLogger

var wg sync.WaitGroup // Block goroutines until user type join
var mutex sync.Mutex  // Mutex used for duplicate update caches write

// Convert struct to byte array
func serialize(data interface{}) []byte {
	buf := bytes.Buffer{}
	binary.Write(&buf, binary.BigEndian, data)
	return buf.Bytes()
}

// Convery byte array to struct
func deserialize(data []byte, sample interface{}) {
	buf := bytes.NewReader(data)
	binary.Read(buf, binary.BigEndian, sample)
}

// A trick to simply get local IP address
func getMyIP() net.IP {
	dial, err := net.Dial("udp", "8.8.8.8:80")
	printError(err)
	localAddr := dial.LocalAddr().(*net.UDPAddr)
	dial.Close()

	return localAddr.IP
}

// Convert net.IP to uint32
func ip2int(ip net.IP) uint32 {
	if len(ip) == 16 {
		return binary.BigEndian.Uint32(ip[12:16])
	} else {
		return binary.BigEndian.Uint32(ip)
	}
}

// Convert uint32 to net.IP
func int2ip(binip uint32) net.IP {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, binip)
	return ip
}

// Helper function to print the err in process
func printError(err error) {
	if err != nil {
		Logger.Error(err.Error())
	}
}

// Send UDP packet
func sendUDP(addr string, packet []byte) {
	conn, err := net.Dial("udp", addr)
	printError(err)
	defer conn.Close()

	conn.Write(packet)
}

func daemon() {
	cc := make(chan string)
	go readCommand(cc)

	wg.Add(1) // Wait until user type join

	go packetListenner() // Packet listening goroutine
	go periodicPing()    // Packet sending goroutine

	commandHandler(cc) // Client goroutine, listen to user's input
}

// Concurrently read user input by chanel
func readCommand(c chan<- string) {
	for {
		var cmd string
		_, err := fmt.Scanln(&cmd)
		printError(err)
		c <- cmd
	}
}

// Handle user input command
func commandHandler(c chan string) {
	for {
		s := <-c
		switch s {

		case "join":
			if MyList.Size() > 0 {
				fmt.Println("Already in the group")
				continue
			}
			wg.Done() // Notify other gorountne that this process have joined the membership

			if MyIP == IntroducerIP {
				MyMember.State |= (StateIntro | StateMonit)
				MyList.Insert(MyMember)
			} else {
				// New member, send Init Request to the introducer
				initRequest(MyMember)
			}

		case "ls":
			MyList.PrintMemberList()

		case "id":
			fmt.Printf("Member (%d, %s)\n", MyMember.Timestamp, MyIP)

		case "leave":
			if MyList.Size() < 1 {
				fmt.Println("Haven't join the group")
				continue
			}
			wg.Add(1)
			initiateLeave()

		case "help":
			fmt.Println("# join")
			fmt.Println("# ls")
			fmt.Println("# id")
			fmt.Println("# leave")

		default:

		}
	}

}

// Periodically ping a randomly selected target
func periodicPing() {
	wg.Wait() // Wait for user to type join

	for {
		// Shuffle membership list and get a member
		// Only executed when the membership list is not empty
		if MyList.Size() > 0 {
			member := MyList.Shuffle()
			// Do not pick itself as the ping target
			if (member.Timestamp == MyMember.Timestamp) && (member.IP == MyMember.IP) {
				time.Sleep(PingSendingPeriod)
				continue
			}
			Logger.Info("Member (%d, %d) is selected by shuffling\n", member.Timestamp, int2ip(member.IP).String())
			// Get update entry from TTL Cache
			update, flag, err := getUpdate()
			// if no update there, do pure ping
			if err != nil {
				ping(member)
			} else {
				// Send update as payload of ping
				pingWithPayload(member, update, flag)
			}
		}
		time.Sleep(PingSendingPeriod)
	}
}

func packetListenner() {
	wg.Wait() // Wait for user to type join

	udpAddr, _ := net.ResolveUDPAddr("udp4", MembershipPort)
	uconn, err := net.ListenUDP("udp", udpAddr)
	printError(err)
	defer uconn.Close()

	// Listening loop
	for {
		packet := make([]byte, 512)               // Buffer to store packet
		n, addr, err := uconn.ReadFromUDP(packet) // Receive packet
		printError(err)

		var header Header
		deserialize(packet[:HeaderLength], &header)

		packetHandler(header, packet[HeaderLength:n], addr) // Maybe concurrent
	}
}

func packetHandler(header Header, payload []byte, addr *net.UDPAddr) {
	if header.Type&Ping != 0 {

		Logger.Info("Receive Ping from %s with seq %d\n", addr.IP.String(), header.Seq)

		if header.Type&MemInitRequest != 0 {
			Logger.Info("Receive init request from %s: with seq %d\n", addr.IP.String(), header.Seq)
			initReply(addr.IP.String(), header.Seq, payload)

		} else if header.Type&MemUpdateSuspect != 0 {
			Logger.Info("Handle suspect update sent from %s\n", addr.IP.String())
			handleSuspect(payload)
			replyAck(header, addr)

		} else if header.Type&MemUpdateResume != 0 {
			Logger.Info("Handle resume update sent from %s\n", addr.IP.String())
			handleResume(payload)
			replyAck(header, addr)

		} else if header.Type&MemUpdateLeave != 0 {
			Logger.Info("Handle leave update sent from %s\n", addr.IP.String())
			handleLeave(payload)
			replyAck(header, addr)

		} else if header.Type&MemUpdateJoin != 0 {
			Logger.Info("Handle join update sent from %s\n", addr.IP.String())
			handleJoin(payload)
			replyAck(header, addr)

		} else {
			Logger.Info("Receive pure ping sent from %s\n", addr.IP.String())
			replyAck(header, addr)
		}

	} else if header.Type&Ack != 0 {

		timer, exist := SuspectTimerMap[ip2int(addr.IP)] // Receive Ack, stop suspect timer
		if exist {
			timer.Stop()
			Logger.Info("Receive Ack from %s with seq %d\n", addr.IP.String(), header.Seq)
			delete(SuspectTimerMap, ip2int(addr.IP)) // Delete timer to avoid memory overflow
		}

		if header.Type&MemInitReply != 0 {
			stop := initTimer.Stop() // Stop init timer
			if stop {
				Logger.Info("Receive init reply from %s with %d\n", addr.IP.String(), header.Seq)
			}
			handleInitReply(payload)

		} else if header.Type&MemUpdateSuspect != 0 {
			Logger.Info("Handle suspect update sent from %s\n", addr.IP.String())
			handleSuspect(payload)

		} else if header.Type&MemUpdateResume != 0 {
			Logger.Info("Handle resume update sent from %s\n", addr.IP.String())
			handleResume(payload)

		} else if header.Type&MemUpdateLeave != 0 {
			Logger.Info("Handle leave update sent from %s\n", addr.IP.String())
			handleLeave(payload)

		} else if header.Type&MemUpdateJoin != 0 {
			Logger.Info("Handle join update sent from %s\n", addr.IP.String())
			handleJoin(payload)

		} else {
			Logger.Info("Receive pure ack sent from %s\n", addr.IP.String())
		}
	}
}

func replyAck(header Header, addr *net.UDPAddr) {
	update, flag, err := getUpdate()
	if err != nil {
		ack(addr.IP.String(), header.Seq)
	} else {
		ackWithPayload(addr.IP.String(), header.Seq, update, flag)
	}
}

// Check whether the update is duplicate
// if duplicated, return false, else, return true and start a timer
func isUpdateDuplicate(id uint64) bool {
	mutex.Lock()
	_, exist := DuplicateUpdateMap[id] // Check existence
	mutex.Unlock()

	if exist {
		Logger.Info("Receive duplicate update %d\n", id)
		return true

	} else {
		mutex.Lock()
		DuplicateUpdateMap[id] = true // Add to cache
		mutex.Unlock()
		Logger.Info("Add update %d to duplicate cache map \n", id)
		updateDeleteTimer := time.NewTimer(UpdateDeletePeriod) // set a delete timer
		go func() {
			<-updateDeleteTimer.C
			mutex.Lock()
			_, exist := DuplicateUpdateMap[id]
			mutex.Unlock()
			if exist {
				mutex.Lock()
				delete(DuplicateUpdateMap, id) // Delete from cache
				mutex.Unlock()
				Logger.Info("Delete update %d from duplicated cache map \n", id)
			}
		}()
		return false
	}
}

func getUpdate() ([]byte, uint8, error) {
	var binBuffer bytes.Buffer

	update, err := UpdateCacheList.Get()
	if err != nil {
		return binBuffer.Bytes(), 0, err
	}

	binary.Write(&binBuffer, binary.BigEndian, update)
	return binBuffer.Bytes(), update.UpdateType, nil
}

// Generate a new update and set it in TTL Cache
func addUpdateToCache(member *Member, updateType uint8) {
	uid := UpdateCacheList.RandGen.Uint64()
	update := Update{uid, TimeToLive, updateType, member.Timestamp, member.IP, member.State}
	UpdateCacheList.Set(&update)
	// This daemon is the update producer, add this update to the update duplicate cache
	isUpdateDuplicate(uid)
}

func handleSuspect(payload []byte) {
	var update Update
	deserialize(payload, &update)

	updateID := update.UpdateID
	if !isUpdateDuplicate(updateID) {
		// If someone suspect me, tell them I am alvie
		if MyMember.Timestamp == update.MemberTimestamp && MyMember.IP == update.MemberIP {
			addUpdateToCache(MyMember, MemUpdateResume)
			return
		}
		// Someone else is being suspected
		MyList.Update(update.MemberTimestamp, update.MemberIP, update.MemberState)
		UpdateCacheList.Set(&update)
		failureTimer := time.NewTimer(SuspectPeriod)
		FailureTimerMap[[2]uint64{update.MemberTimestamp, uint64(update.MemberIP)}] = failureTimer
		go func() {
			<-failureTimer.C
			err := MyList.Delete(update.MemberTimestamp, update.MemberIP)
			if err == nil {
				Logger.Info("[Failure Detected](%s, %d) Failed, detected by suspect update\n", int2ip(update.MemberIP).String(), update.MemberTimestamp)
			}
			delete(FailureTimerMap, [2]uint64{update.MemberTimestamp, uint64(update.MemberIP)})
		}()
	}
}

func handleResume(payload []byte) {
	var update Update
	deserialize(payload, &update)

	updateID := update.UpdateID
	if !isUpdateDuplicate(updateID) {
		suspectTimer, exist := SuspectTimerMap[update.MemberIP]
		if exist {
			suspectTimer.Stop()
			delete(SuspectTimerMap, update.MemberIP)
		}
		failureTimer, exist := FailureTimerMap[[2]uint64{update.MemberTimestamp, uint64(update.MemberIP)}]
		if exist {
			failureTimer.Stop()
			delete(FailureTimerMap, [2]uint64{update.MemberTimestamp, uint64(update.MemberIP)})
		}
		err := MyList.Update(update.MemberTimestamp, update.MemberIP, update.MemberState)
		// If the resume target is not in the list, insert it to the list
		if err != nil {
			MyList.Insert(&Member{update.MemberTimestamp, update.MemberIP, update.MemberState})
		}
		UpdateCacheList.Set(&update)
	}
}

func handleLeave(payload []byte) {
	var update Update
	deserialize(payload, &update)

	updateID := update.UpdateID
	if !isUpdateDuplicate(updateID) {
		MyList.Delete(update.MemberTimestamp, update.MemberIP)
		UpdateCacheList.Set(&update)
	}
}

func handleJoin(payload []byte) {
	var update Update
	deserialize(payload, &update)

	updateID := update.UpdateID
	if !isUpdateDuplicate(updateID) {
		MyList.Insert(&Member{update.MemberTimestamp, update.MemberIP, update.MemberState})
		UpdateCacheList.Set(&update)
	}
}

func handleInitReply(payload []byte) {
	num := len(payload) / 13 // 13 bytes per member
	buf := bytes.NewReader(payload)
	for idx := 0; idx < num; idx++ {
		var member Member
		binary.Read(buf, binary.BigEndian, &member)
		MyList.Insert(&member) // Insert other member to the new member's memberlist
	}
}

// Introducer replies new node join init request and send the new node join updates to others members
func initReply(addr string, seq uint16, payload []byte) {
	var newMember Member
	deserialize(payload, &newMember)
	MyList.Insert(&newMember)
	addUpdateToCache(&newMember, MemUpdateJoin) // Update this new member's join

	// Put the entire memberlist to the Init Reply's payload
	var memBuffer bytes.Buffer // Temp buf to store member's binary value
	var binBuffer bytes.Buffer

	for i := 0; i < MyList.Size(); i += 1 {
		member, _ := MyList.RetrieveByIdx(i)
		binary.Write(&memBuffer, binary.BigEndian, member)
		binBuffer.Write(memBuffer.Bytes()) // Append existing member's bytes into binBuffer
		memBuffer.Reset()                  // Clear buffer
	}

	// Send pigggback Init Reply
	ackWithPayload(addr, seq, binBuffer.Bytes(), MemInitReply)
}

func initRequest(member *Member) {
	// Send piggyback init request
	pingWithPayload(&Member{0, ip2int(net.ParseIP(IntroducerIP)), 0}, serialize(member), MemInitRequest)

	// Start init timer, exit process if the timer expires, init timer stops when the daemon receives init reply
	initTimer = time.NewTimer(InitTimeoutPeriod)
	go func() {
		<-initTimer.C
		Logger.Info("Init %s timeout, process exit\n", IntroducerIP)
		os.Exit(1)
	}()
}

func initiateLeave() {
	uid := UpdateCacheList.RandGen.Uint64()
	update := Update{uid, TimeToLive, MemUpdateLeave, MyMember.Timestamp, MyMember.IP, MyMember.State}
	// Clear current update cache list and add delete update to the cache
	UpdateCacheList = NewTtlCache()
	UpdateCacheList.Set(&update)
	isUpdateDuplicate(uid)
	Logger.Info("Member (%d, %s) leaves", MyMember.Timestamp, MyIP)
	time.Sleep(LeaveDelayPeriod)
	Initilize()
}

func ackWithPayload(addr string, seq uint16, payload []byte, flag uint8) {
	header := Header{
		Type:     Ack | flag,
		Seq:      seq + 1,
		Reserved: 0,
	}

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, header)

	if payload != nil {
		buf.Write(payload) // Append payload
		sendUDP(addr+MembershipPort, buf.Bytes())
	} else {
		sendUDP(addr+MembershipPort, buf.Bytes())
	}
}

func ack(addr string, seq uint16) {
	ackWithPayload(addr, seq, nil, 0x00)
}

func pingWithPayload(member *Member, payload []byte, flag uint8) {
	// Generating random sequence number
	randSource := rand.NewSource(time.Now().UnixNano())
	randGen := rand.New(randSource)
	seq := randGen.Intn(0x01<<15 - 2)
	addr := int2ip(member.IP).String() + MembershipPort

	header := Header{
		Type:     Ping | flag,
		Seq:      uint16(seq),
		Reserved: 0,
	}

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, header)

	if payload != nil {
		buf.Write(payload) // Append payload
		sendUDP(addr, buf.Bytes())
	} else {
		sendUDP(addr, buf.Bytes())
	}
	Logger.Info("Ping (%s, %d)\n", addr, seq)

	// Set timer to detect failure. Use target IP to track timer
	_, exist := SuspectTimerMap[member.IP]
	if !exist {
		suspectTimer := time.NewTimer(PingTimeoutPeriod)
		SuspectTimerMap[member.IP] = suspectTimer
		go func() {
			<-suspectTimer.C
			Logger.Info("Ping (%s, %d) timeout\n", addr, seq)
			err := MyList.Update(member.Timestamp, member.IP, StateSuspect)
			if err == nil {
				addUpdateToCache(member, MemUpdateSuspect)
			}
			delete(SuspectTimerMap, member.IP)
			// Handle local suspect timeout
			failureTimer := time.NewTimer(SuspectPeriod)
			FailureTimerMap[[2]uint64{member.Timestamp, uint64(member.IP)}] = failureTimer
			go func() {
				<-failureTimer.C
				err := MyList.Delete(member.Timestamp, member.IP)
				if err == nil {
					Logger.Info("[Failure Detected](%s, %d) Failed, detected by self\n", int2ip(member.IP).String(), member.Timestamp)
				}
				delete(FailureTimerMap, [2]uint64{member.Timestamp, uint64(member.IP)})
			}()
		}()
	}
}

func ping(member *Member) {
	pingWithPayload(member, nil, 0x00)
}

// Start the membership service and join in the group
func Initilize() bool {
	Logger = NewSsmsLogger(MyIP)

	// Create my member entry
	MyIP = getMyIP().String()
	timestamp := time.Now().UnixNano()
	state := StateAlive | StateMonit
	MyMember = &Member{uint64(timestamp), ip2int(getMyIP()), uint8(state)}

	// Create member list
	MyList = NewMemberList(20)

	// Make necessary Maps
	SuspectTimerMap = make(map[uint32]*time.Timer)
	FailureTimerMap = make(map[[2]uint64]*time.Timer)
	DuplicateUpdateMap = make(map[uint64]bool)
	UpdateCacheList = NewTtlCache()

	return true
}

// Main func
func Start(introducerIP, port string) {
	IntroducerIP = introducerIP
	MembershipPort = ":" + port

	// Start daemon
	daemon()
}
