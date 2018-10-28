package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"crypto/sha256"
	"net"
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

func ParseFilename(data [128]byte) string {
	n := bytes.IndexByte([]byte(data[:]), 0)
	filename := fmt.Sprintf("%s", data[:n])
	return filename
}

func HashFilename(filename string) [32]byte {
	hash := sha256.Sum256([]byte(filename))
	return hash
}

func BinaryIP(IP string) uint32 {
	return binary.BigEndian.Uint32(net.ParseIP(IP).To4())
}

func StringIP(binIP uint32) string {
	IP := make(net.IP, 4)
	binary.BigEndian.PutUint32(IP, binIP)
	return IP.String()
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
