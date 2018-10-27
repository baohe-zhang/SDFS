package main

import (
	"fmt"
	"os"
)

// Usage of correct client command
func usage() {
	fmt.Println("Usage of ./client")
	fmt.Println("    put [localfilename] [sdfsfilename]")
	fmt.Println("    get [sdfsfilename] [localfilename]")
	fmt.Println("    delete [sdfsfilename]")
	fmt.Println("    ls [sdfsfilename]")
	fmt.Println("    store")
	fmt.Println("    get-versions [sdfsfilename] [num-versions] [localfilename]")
}

func main() {
	if len(os.Args) <= 1 {
		usage()
		return
	}
	args := os.Args[1:]

	command := args[0]
	switch command {
	case "put":
		if len(args) != 3 {
			fmt.Println("Invalid put usage")
			usage()
		}
		fmt.Println(args[1:])
	case "get":
		if len(args) != 3 {
			fmt.Println("Invalid get usage")
			usage()
		}
		fmt.Println(args[1:])
	case "delete":
		if len(args) != 2 {
			fmt.Println("Invalid delete usage")
			usage()
		}
		fmt.Println(args[1:])
	case "ls":
		if len(args) != 2 {
			fmt.Println("Invalid ls usage")
			usage()
		}
		fmt.Println(args[1:])
	case "store":
		if len(args) != 1 {
			fmt.Println("Invalid store usage")
			usage()
		}
		fmt.Println(args[1:])
	case "get-versions":
		if len(args) != 4 {
			fmt.Println("Invalid get-versions usage")
			usage()
		}
		fmt.Println(args[1:])
	default:
		usage()
	}

}
