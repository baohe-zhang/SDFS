package utils

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type Meta map[string]Infos

type Infos []Info

type Info struct {
	Timestamp uint64
	Filesize  uint64
	DataNodes []NodeID
}

func NewMeta(filename string) Meta {
	var file *os.File
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		file, _ = os.Create(filename)
	} else {
		file, _ = os.Open(filename)
	}
	defer file.Close()

	meta := Meta{}
	b, _ := ioutil.ReadAll(file)
	json.Unmarshal(b, &meta)

	return meta
}

func (meta Meta) StoreMeta(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file.Close()

	b, _ := json.Marshal(meta)
	file.Write(b)
}

func (meta Meta) FileInfo(filename string) (Info, bool) {
	val, ok := meta[filename]
	if ok {
		return val[0], true
	} else {
		return Info{}, false
	}
}

func (meta Meta) FileInfoWithTs(filename string, ts uint64) (Info, bool) {
	infos, ok := meta[filename]
	if ok {
		for _, info := range infos {
			if info.Timestamp == ts {
				fmt.Println("Successful return", info, ts)
				return info, true
			}
		}
	}
	fmt.Println("UnSuccessful return", ts)
	return Info{}, false
}

func (meta Meta) FileInfos(filename string) (Infos, bool) {
	val, ok := meta[filename]
	if ok {
		if len(val) < 5 {
			return val, true
		} else {
			return val[:5], true
		}
	} else {
		return Infos{}, false
	}
}

func (meta Meta) PutFileInfo(filename string, info Info) {
	meta[filename] = append(meta[filename], info)
	meta.SortFileInfo(filename)
}

func (meta Meta) UpdateFileInfo(filename string, dataNodeList []NodeID) {
	val, ok := meta[filename]
	if ok {
		for i := 0; i < 5; i++ {
			if i >= len(val) {
				break
			}
			for j, _ := range val[i].DataNodes {
				val[i].DataNodes[j] = dataNodeList[j]
			}
		}
	}
}

func (meta Meta) UpdateFileInfoWithTs(filename string, dataNodeList []NodeID, ts uint64) {
	infos, ok := meta[filename]
	if ok {
		for _, info := range infos {
			if info.Timestamp == ts {
				for j, _ := range info.DataNodes {
					info.DataNodes[j] = dataNodeList[j]
				}
			}
		}
	}
}

func (meta Meta) RmFileInfo(filename string) (Infos, bool) {
	infos, ok := meta[filename]
	if ok {
		delete(meta, filename)
		return infos, true
	}
	return Infos{}, false
}

func (meta Meta) FilesIn(clientIP uint32) []string {
	res := make([]string, 0)
	for key, value := range meta {
		inThisClient := false
		for _, info := range value {
			for _, node := range info.DataNodes {
				if node.IP == clientIP {
					inThisClient = true
					break
				}
				if inThisClient {
					break
				}
			}
			if inThisClient {
				break
			}
		}
		if inThisClient {
			res = append(res, key)
		}
	}
	return res
}

func (meta Meta) SortFileInfo(filename string) {
	infos := meta[filename]
	n := len(infos)

	// Bubble Sort
	swapped := false
	for i := 0; i < n-1; i++ {
		swapped = false
		for j := 0; j < n-1-i; j++ {
			if infos[j].Timestamp < infos[j+1].Timestamp {
				infos[j], infos[j+1] = infos[j+1], infos[j]
				swapped = true
			}
		}
		if !swapped {
			break
		}
	}
}

// Test client

// func main() {
// 	meta := NewMeta("meta.json")

// 	info := Info{
// 		Timestamp: 20,
// 		Filesize:  32,
// 		DataNodes: []uint8{4, 5, 8, 9},
// 	}

// 	meta.PutFileInfo("file3", info)
// 	fmt.Println(meta["file1"])

// 	meta.StoreMeta("meta2.json")

// 	meta2 := NewMeta("meta2.json")
// 	fmt.Println(meta2["file1"])
// }
