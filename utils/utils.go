package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
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

// Test client
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
