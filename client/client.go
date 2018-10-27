package main

import (
	"flag"
	"fmt"
	"os"
)

// Usage of correct client command
func usage() {
	fmt.Println("Usage of ./client")
	fmt.Println("   -master=[master IP:Port] put [localfilename] [sdfsfilename]")
	fmt.Println("   -master=[master IP:Port] get [sdfsfilename] [localfilename]")
	fmt.Println("   -master=[master IP:Port] delete [sdfsfilename]")
	fmt.Println("   -master=[master IP:Port] ls [sdfsfilename]")
	fmt.Println("   -master=[master IP:Port] store")
	fmt.Println("   -master=[master IP:Port] get-versions [sdfsfilename] [num-versions] [localfilename]")
}

func contactMaster(masterIP string) {

}

func main() {
	// If no command line arguments, return
	if len(os.Args) <= 1 {
		usage()
		return
	}
	ipPtr := flag.String("master", "xx.xx.xx.xx", "Master's IP address")
	flag.Parse()
	masterIP := *ipPtr
	fmt.Println("Master IP address ", masterIP)
	args := flag.Args()

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

// Helper function to print the err in process
func printError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n[ERROR]", err.Error())
	}
}
