// Aether CLI — High-performance file transfer tool.
//
// Built with spf13/cobra. Uses pipelined architecture: chunking and
// uploading happen concurrently for maximum throughput.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/Aryan-Protein-Vala/AetherNet/pkg/chunker"
	"github.com/Aryan-Protein-Vala/AetherNet/pkg/network"
	"github.com/Aryan-Protein-Vala/AetherNet/pkg/receiver"
)

const version = "0.2.0-alpha"

var (
	cyan    = color.New(color.FgCyan, color.Bold).SprintFunc()
	green   = color.New(color.FgGreen, color.Bold).SprintFunc()
	red     = color.New(color.FgRed, color.Bold).SprintFunc()
	dim     = color.New(color.Faint).SprintFunc()
	bold    = color.New(color.Bold).SprintFunc()
	yellow  = color.New(color.FgYellow).SprintFunc()
	magenta = color.New(color.FgMagenta, color.Bold).SprintFunc()
)

func main() {
	root := &cobra.Command{
		Use:   "aether",
		Short: "Aether -- High-Performance File Transfer",
		Long:  banner(),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(banner())
		},
	}

	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print Aether version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("  %s %s\n", cyan("aether"), dim("v"+version))
		},
	})

	sendCmd := &cobra.Command{
		Use:   "send [filepath]",
		Short: "Chunk and upload a file to an Aether receiver",
		Long:  "  Split a file and upload via pipelined parallel streams.\n\n  Example:\n    aether send model.bin --to http://localhost:8080",
		Args:  cobra.ExactArgs(1),
		RunE:  runSend,
	}
	sendCmd.Flags().StringP("to", "t", "http://localhost:8080", "Target receiver URL")
	sendCmd.Flags().Uint32P("chunk-size", "c", 0, "Chunk size in bytes (default: 2MB)")
	sendCmd.Flags().IntP("workers", "w", 5, "Number of parallel upload workers")
	root.AddCommand(sendCmd)

	receiveCmd := &cobra.Command{
		Use:   "receive",
		Short: "Start the Aether chunk receiver server",
		Long:  "  Start an HTTP server that accepts incoming chunk uploads.\n\n  Example:\n    aether receive --port 8080",
		RunE:  runReceive,
	}
	receiveCmd.Flags().IntP("port", "p", 8080, "Port to listen on")
	root.AddCommand(receiveCmd)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

// ──────────────────────────────────────────────────────────────────────
// send — pipelined: chunk + upload happen concurrently
// ──────────────────────────────────────────────────────────────────────

func runSend(cmd *cobra.Command, args []string) error {
	filePath := args[0]
	targetURL, _ := cmd.Flags().GetString("to")
	chunkSizeFlag, _ := cmd.Flags().GetUint32("chunk-size")
	workers, _ := cmd.Flags().GetInt("workers")

	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("resolve path: %w", err)
	}

	info, err := os.Stat(absPath)
	if err != nil {
		printError("File not found: %s", absPath)
		return err
	}

	// ── Header ───────────────────────────────────────────────────────
	printHeader()
	fmt.Println()
	printKV("File", filepath.Base(absPath))
	printKV("Size", formatBytes(info.Size()))
	printKV("Target", targetURL)
	printKV("Workers", fmt.Sprintf("%d goroutines", workers))
	printKV("Hash", "BLAKE3-256")
	printKV("Mode", "pipelined (chunk + upload concurrent)")
	fmt.Println()

	// ── Start pipelined transfer ─────────────────────────────────────
	printStep("Chunking + uploading (pipelined)")
	totalStart := time.Now()

	pipe, err := chunker.ChunkFilePipelined(absPath, chunkSizeFlag)
	if err != nil {
		printError("Pipeline init failed: %v", err)
		return err
	}

	printSuccess("Pipeline started: %s chunks (%s each)",
		bold(fmt.Sprintf("%d", pipe.TotalChunks)),
		formatBytes(int64(pipe.ChunkSize)),
	)

	stats, err := network.UploadPipelined(pipe, targetURL, workers)
	if err != nil {
		printError("Transfer failed: %v", err)
		return err
	}

	totalDur := time.Since(totalStart)

	// ── Summary ──────────────────────────────────────────────────────
	fmt.Println()
	printDivider()
	fmt.Println()

	if stats.FailCount == 0 {
		fmt.Printf("  %s Transfer complete\n\n", green("✓"))
	} else {
		fmt.Printf("  %s Transfer completed with %d failed chunk(s)\n\n",
			yellow("!"), stats.FailCount)
	}

	mbPerSec := 0.0
	if totalDur.Seconds() > 0 {
		mbPerSec = float64(stats.TotalBytes) / (1024 * 1024) / totalDur.Seconds()
	}

	printKV("Chunks", fmt.Sprintf("%d/%d %s",
		stats.SuccessCount, stats.TotalChunks, dim("verified")))
	printKV("Transferred", formatBytes(stats.TotalBytes))
	printKV("Total Time", bold(formatDuration(totalDur)))
	printKV("Throughput", fmt.Sprintf("%s %s",
		magenta(fmt.Sprintf("%.2f MB/s", mbPerSec)), dim("(avg)")))
	fmt.Println()
	printKV("Destination", dim(targetURL))
	fmt.Println()

	return nil
}

// ──────────────────────────────────────────────────────────────────────
// receive
// ──────────────────────────────────────────────────────────────────────

func runReceive(cmd *cobra.Command, args []string) error {
	port, _ := cmd.Flags().GetInt("port")
	srv := receiver.New(port)
	return srv.Start()
}

// ──────────────────────────────────────────────────────────────────────
// Pretty-print helpers
// ──────────────────────────────────────────────────────────────────────

func banner() string {
	return fmt.Sprintf(`
  %s  %s
  %s
`,
		cyan("▲"), bold("Aether"),
		dim("High-Performance File Transfer Engine"),
	)
}

func printHeader() {
	fmt.Print(banner())
}

func printStep(msg string) {
	fmt.Printf("  %s %s\n", cyan(">>"), msg)
}

func printSuccess(format string, args ...interface{}) {
	fmt.Printf("  %s %s\n", green("✓"), fmt.Sprintf(format, args...))
}

func printError(format string, args ...interface{}) {
	fmt.Printf("  %s %s\n", red("✗"), fmt.Sprintf(format, args...))
}

func printKV(key, value string) {
	padding := 14 - len(key)
	if padding < 1 {
		padding = 1
	}
	fmt.Printf("  %s%s%s\n", dim(key), strings.Repeat(" ", padding), value)
}

func printDivider() {
	fmt.Printf("  %s\n", dim("────────────────────────────────────────"))
}

func formatBytes(b int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func formatDuration(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%.0fus", float64(d.Microseconds()))
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}
