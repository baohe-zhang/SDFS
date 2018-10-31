package utils

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"os/exec"
)

func Serialize(data interface{}) []byte {
	buf := bytes.Buffer{}
	binary.Write(&buf, binary.BigEndian, data)
	return buf.Bytes()
}

func Deserialize(data []byte, sample interface{}) {
	buf := bytes.NewReader(data)
	binary.Read(buf, binary.BigEndian, sample)
}

func ParseFilename(data []byte) string {
	n := bytes.IndexByte(data, 0)
	filename := fmt.Sprintf("%s", data[:n])
	return filename
}

func HashFilename(filename string) [32]byte {
	hash := sha256.Sum256([]byte(filename))
	return hash
}

func StringHashFilename(hash []byte) string {
	return fmt.Sprintf("%x", hash)
}

func Hash2Text(hashcode []byte) string {
	return base64.URLEncoding.EncodeToString(hashcode)
}

func BinaryIP(IP string) uint32 {
	return binary.BigEndian.Uint32(net.ParseIP(IP).To4())
}

func StringIP(binIP uint32) string {
	IP := make(net.IP, 4)
	binary.BigEndian.PutUint32(IP, binIP)
	return IP.String()
}

func StringPort(binPort uint16) string {
	return fmt.Sprint(binPort)
}

// Helper function to generate a certain range of replica for a specific filename
func HashReplicaRange(filename string, capacity uint32) ([NumReplica]uint8, error) {
	var res [NumReplica]uint8
	if capacity <= 0 {
		return res, errors.New("Capacity is lower than 1")
	}
	h := fnv.New32a()
	h.Write([]byte(filename))
	hashcode := h.Sum32()

	start := (hashcode + 5) >> 5 % capacity
	end := start + NumReplica
	for i := start; i < end; i++ {
		res[i-start] = uint8(i % capacity)
	}
	return res, nil
}

// Helper function to print the err in process
func PrintError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n[ERROR]", err.Error())
		fmt.Println(" ")
	}
}

// A trick to simply get local IP address
func GetLocalIP() net.IP {
	dial, err := net.Dial("udp", "8.8.8.8:80")
	PrintError(err)
	localAddr := dial.LocalAddr().(*net.UDPAddr)
	dial.Close()

	return localAddr.IP
}

// Return local FQDN
func GetLocalHostname() string {
	cmd := exec.Command("/bin/hostname", "-f")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
	    fmt.Println(err.Error())
	}
	hostname := out.String()
	hostname = hostname[:len(hostname)-1] // removing EOL

	return hostname
}

func LookupIP(hostname string) string {
	addrs, err := net.LookupHost(hostname)
	if err != nil {
		fmt.Println(err.Error())
	}

	return addrs[0]
}

// Test BinaryIP()
// func main() {
// 	b := BinaryIP("10.193.185.82")
// 	fmt.Printf("%x\n", b)
// 	fmt.Println(StringIP(b))
// }

// Test HashFilename
// func main() {
// 	filename := "user/hello.txt"
// 	fmt.Println(len(HashFilename(filename)))
// 	fmt.Printf("%x", HashFilename(filename))
// }

// Test Serialize(), Deserialize()
// func main() {
// 	putReq := daemon.PutRequest{
// 		Filesize: uint32(10240),
// 	}
// 	copy(putReq.Filename[:], "myFilename")

// 	b := Serialize(putReq)
// 	fmt.Printf("%v", b)

// 	s := daemon.PutRequest{}
// 	Deserialize(b, &s)
// 	fmt.Println(s.Filesize)
// }
