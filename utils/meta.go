package utils

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type Meta struct {
	Files []File
}

type File struct {
	Filename string
	Infos    []Info
}

type Info struct {
	Timestamp uint64
	Filesize  uint64
	DataNodes []uint8
}

// Test client
// func main() {

// 	file, err := os.Open("meta.json")
// 	if err != nil {
// 		fmt.Println(err.Error())
// 	}
// 	defer file.Close()

// 	var meta Meta
// 	b, _ := ioutil.ReadAll(file)
// 	json.Unmarshal(b, &meta)

// 	var target_info Info

// 	// Get most up to date info
// 	for _, file := range meta.Files {
// 		if file.Filename == "file1" {
// 			target_info = file.Infos[0]
// 			for _, info := range file.Infos {
// 				if info.Timestamp > target_info.Timestamp {
// 					target_info = info
// 				}
// 			}
// 			fmt.Println(target_info.Filesize)
// 			break
// 		}
// 	}
// }
