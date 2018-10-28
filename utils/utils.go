package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"crypto/sha256"
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
