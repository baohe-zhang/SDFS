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
	DataNodes []uint8
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
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file.Close()

	b, _ := json.Marshal(meta)
	file.Write(b[:])
}

func (meta Meta) FileInfo(filename string) Info {
	return meta[filename][0]
}

func (meta Meta) PutFileInfo(filename string, info Info) {
	meta[filename] = append(meta[filename], info)
	meta.SortFileInfo(filename)
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
/*
func main() {
	meta := NewMeta("meta3.json")

	info := Info{
		Timestamp: 20,
		Filesize:  32,
		DataNodes: []uint8{1, 2, 3, 4},
	}

	meta.PutFileInfo("file1", info)
	fmt.Println(meta["file1"])
}
*/
