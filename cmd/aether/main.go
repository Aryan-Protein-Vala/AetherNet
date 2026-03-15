// Aether CLI — High-performance file transfer tool.
//
// Usage:
//
//	aether send <file> --to <destination>
//	aether receive --session <id>
package main

import (
	"fmt"
	"os"

	"github.com/yourusername/aether/pkg/chunker"
)

const version = "0.1.0-alpha"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(0)
	}

	switch os.Args[1] {
	case "version":
		fmt.Printf("aether v%s\n", version)
	case "send":
		fmt.Println("⚡ aether send — coming soon")
		fmt.Printf("  Chunk size range: %d KB – %d KB\n",
			chunker.MinChunkSize/1024,
			chunker.MaxChunkSize/1024,
		)
	case "receive":
		fmt.Println("⬇  aether receive — coming soon")
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`
  ___       _   _               
 / _ \     | | | |              
/ /_\ \ ___| |_| |__   ___ _ __ 
|  _  |/ _ \ __| '_ \ / _ \ '__|
| | | |  __/ |_| | | |  __/ |   
\_| |_/\___|\__|_| |_|\___|_|   

Aether — High-Performance File Transfer

Usage:
  aether send <file> --to <destination>
  aether receive --session <id>
  aether version

Flags:
  --chunk-size   Chunk size in MB (default: 2)
  --streams      Parallel streams (default: 8)
  --help         Show this help message`)
}
