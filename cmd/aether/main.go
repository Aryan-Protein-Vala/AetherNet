// Aether CLI — High-performance file transfer via relay architecture.
//
// Commands:
//   aether send  <file> --to <relay>     Upload to relay
//   aether fetch <fileID> --from <relay>  Download from relay
//   aether relay --port <port>           Start a relay server
//   aether version                       Print version
package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/Aryan-Protein-Vala/AetherNet/pkg/chunker"
	aecrypto "github.com/Aryan-Protein-Vala/AetherNet/pkg/crypto"
	"github.com/Aryan-Protein-Vala/AetherNet/pkg/network"
	"github.com/Aryan-Protein-Vala/AetherNet/pkg/receiver"
)

const (
	version         = "0.3.0-alpha"
	DefaultRelayURL = "http://localhost:8080"
)

var (
	cyanC   = color.New(color.FgCyan, color.Bold).SprintFunc()
	greenC  = color.New(color.FgGreen, color.Bold).SprintFunc()
	redC    = color.New(color.FgRed, color.Bold).SprintFunc()
	dimC    = color.New(color.Faint).SprintFunc()
	boldC   = color.New(color.Bold).SprintFunc()
	yellowC = color.New(color.FgYellow).SprintFunc()
	magC    = color.New(color.FgMagenta, color.Bold).SprintFunc()
	whiteB  = color.New(color.FgWhite, color.Bold, color.BgMagenta).SprintFunc()
)

func main() {
	root := &cobra.Command{
		Use:   "aether",
		Short: "Aether -- High-Performance File Transfer",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Print(banner())
		},
	}

	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print Aether version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("  %s %s\n", cyanC("aether"), dimC("v"+version))
		},
	})

	// ── send ──────────────────────────────────────────────────────────
	sendCmd := &cobra.Command{
		Use:   "send [filepath]",
		Short: "Chunk and upload a file to an Aether relay",
		Args:  cobra.ExactArgs(1),
		RunE:  runSend,
	}
	sendCmd.Flags().StringP("to", "t", DefaultRelayURL, "Relay server URL")
	sendCmd.Flags().IntP("workers", "w", 5, "Parallel upload workers")
	sendCmd.Flags().BoolP("compress", "z", false, "Enable LZ4 compression")
	sendCmd.Flags().BoolP("encrypt", "e", false, "Enable AES-256-GCM encryption")
	sendCmd.Flags().StringP("password", "k", "", "Encryption passphrase")
	root.AddCommand(sendCmd)

	// ── fetch ─────────────────────────────────────────────────────────
	fetchCmd := &cobra.Command{
		Use:   "fetch [fileID]",
		Short: "Download a file from an Aether relay and reassemble it",
		Args:  cobra.ExactArgs(1),
		RunE:  runFetch,
	}
	fetchCmd.Flags().StringP("from", "f", DefaultRelayURL, "Relay server URL")
	fetchCmd.Flags().StringP("out", "o", ".", "Output directory")
	fetchCmd.Flags().IntP("workers", "w", 5, "Parallel download workers")
	fetchCmd.Flags().StringP("password", "k", "", "Decryption passphrase")
	root.AddCommand(fetchCmd)

	// ── relay ─────────────────────────────────────────────────────────
	relayCmd := &cobra.Command{
		Use:   "relay",
		Short: "Start an Aether relay server",
		RunE:  runRelay,
	}
	relayCmd.Flags().IntP("port", "p", 8080, "Port to listen on")
	root.AddCommand(relayCmd)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

// ──────────────────────────────────────────────────────────────────────
// send — upload to relay
// ──────────────────────────────────────────────────────────────────────

func runSend(cmd *cobra.Command, args []string) error {
	filePath := args[0]
	targetURL, _ := cmd.Flags().GetString("to")
	workers, _ := cmd.Flags().GetInt("workers")
	compress, _ := cmd.Flags().GetBool("compress")
	encrypt, _ := cmd.Flags().GetBool("encrypt")
	password, _ := cmd.Flags().GetString("password")

	if encrypt && password == "" {
		printError("--encrypt requires --password")
		return fmt.Errorf("encryption requires a passphrase")
	}

	opts := &network.TransferOptions{Compress: compress, Encrypt: encrypt}
	if encrypt {
		opts.EncryptKey = aecrypto.DeriveKey(password)
	}

	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("resolve path: %w", err)
	}

	info, err := os.Stat(absPath)
	if err != nil {
		printError("File not found: %s", absPath)
		return err
	}

	printHeader()
	fmt.Println()
	printKV("File", filepath.Base(absPath))
	printKV("Size", formatBytes(info.Size()))
	printKV("Relay", targetURL)
	printKV("Workers", fmt.Sprintf("%d goroutines", workers))
	printKV("Hash", "SHA-256")
	if compress {
		printKV("Compress", "LZ4")
	}
	if encrypt {
		printKV("Encrypt", "AES-256-GCM")
	}
	printKV("Mode", "pipelined")
	fmt.Println()

	printStep("Chunking + uploading (pipelined)")
	totalStart := time.Now()

	pipe, err := chunker.ChunkFilePipelined(absPath, 0)
	if err != nil {
		printError("Pipeline init failed: %v", err)
		return err
	}

	printSuccess("Pipeline started: %s chunks (%s each)",
		boldC(fmt.Sprintf("%d", pipe.TotalChunks)),
		formatBytes(int64(pipe.ChunkSize)),
	)

	stats, err := network.UploadPipelined(pipe, targetURL, workers, opts)
	if err != nil {
		printError("Transfer failed: %v", err)
		return err
	}

	// Send manifest to relay (no reassembly trigger)
	if stats.FailCount == 0 {
		normalizedURL := strings.TrimRight(targetURL, "/")
		manifestJSON := fmt.Sprintf(
			`{"file_name":"%s","file_size":%d,"total_chunks":%d,"chunk_size":%d,"compressed":%t,"encrypted":%t}`,
			pipe.FileName, pipe.FileSize, pipe.TotalChunks, pipe.ChunkSize, compress, encrypt,
		)
		req, _ := http.NewRequest(http.MethodPost,
			normalizedURL+"/manifest",
			strings.NewReader(manifestJSON),
		)
		req.Header.Set("X-Aether-File-ID", pipe.FileIDHex)
		req.Header.Set("Content-Type", "application/json")
		http.DefaultClient.Do(req)
	}

	totalDur := time.Since(totalStart)

	// ── Summary ──────────────────────────────────────────────────────
	fmt.Println()
	printDivider()
	fmt.Println()

	if stats.FailCount == 0 {
		fmt.Printf("  %s Transfer complete\n\n", greenC("✓"))
	} else {
		fmt.Printf("  %s Transfer completed with %d failed chunk(s)\n\n",
			yellowC("!"), stats.FailCount)
	}

	mbPerSec := 0.0
	if totalDur.Seconds() > 0 {
		mbPerSec = float64(stats.TotalBytes) / (1024 * 1024) / totalDur.Seconds()
	}

	printKV("Chunks", fmt.Sprintf("%d/%d %s",
		stats.SuccessCount, stats.TotalChunks, dimC("verified")))
	printKV("Transferred", formatBytes(stats.TotalBytes))
	printKV("Total Time", boldC(formatDuration(totalDur)))
	printKV("Throughput", fmt.Sprintf("%s %s",
		magC(fmt.Sprintf("%.2f MB/s", mbPerSec)), dimC("(avg)")))
	fmt.Println()

	// ── Share instruction ────────────────────────────────────────────
	if stats.FailCount == 0 {
		fmt.Printf("  %s Tell your recipient to run:\n\n", cyanC("→"))
		fmt.Printf("    %s\n\n",
			whiteB(fmt.Sprintf(" aether fetch %s ", pipe.FileIDHex)))
	}

	return nil
}

// ──────────────────────────────────────────────────────────────────────
// fetch — download from relay + reassemble
// ──────────────────────────────────────────────────────────────────────

func runFetch(cmd *cobra.Command, args []string) error {
	fileID := args[0]
	relayURL, _ := cmd.Flags().GetString("from")
	outputDir, _ := cmd.Flags().GetString("out")
	workers, _ := cmd.Flags().GetInt("workers")
	password, _ := cmd.Flags().GetString("password")

	// Build download options — encryption key provided by user if needed.
	// Compress flag is auto-detected from the manifest on the relay.
	opts := &network.TransferOptions{}
	if password != "" {
		opts.Encrypt = true
		opts.EncryptKey = aecrypto.DeriveKey(password)
	}

	printHeader()
	fmt.Println()
	printKV("File ID", fileID)
	printKV("Relay", relayURL)
	printKV("Workers", fmt.Sprintf("%d goroutines", workers))
	if password != "" {
		printKV("Decrypt", "AES-256-GCM")
		printKV("Decompress", "LZ4 (if applicable)")
	}
	printKV("Output", outputDir)
	fmt.Println()

	// ── Fetch manifest ───────────────────────────────────────────────
	printStep("Fetching manifest...")
	totalStart := time.Now()

	manifest, dlStats, err := network.DownloadPipelined(fileID, relayURL, workers, opts)
	if err != nil {
		printError("Download failed: %v", err)
		return err
	}

	if manifest != nil {
		printSuccess("File: %s (%s, %d chunks)",
			boldC(manifest.FileName),
			formatBytes(manifest.FileSize),
			manifest.TotalChunks,
		)
	}

	// ── Reassemble ───────────────────────────────────────────────────
	fmt.Println()
	printStep("Reassembling file...")

	// We need to copy manifest.json into the cache dir so Reassemble can find the filename
	cacheManifestDir := filepath.Join(".aether_cache", fileID)
	os.MkdirAll(cacheManifestDir, 0o755)
	if manifest != nil {
		manifestJSON := fmt.Sprintf(
			`{"file_name":"%s","file_size":%d,"total_chunks":%d,"chunk_size":%d}`,
			manifest.FileName, manifest.FileSize, manifest.TotalChunks, manifest.ChunkSize,
		)
		os.WriteFile(
			filepath.Join(cacheManifestDir, "manifest.json"),
			[]byte(manifestJSON), 0o644,
		)
	}

	outPath, err := receiver.ReassembleFromCache(fileID, outputDir)
	if err != nil {
		printError("Reassembly failed: %v", err)
		return err
	}

	totalDur := time.Since(totalStart)

	// ── Summary ──────────────────────────────────────────────────────
	fmt.Println()
	printDivider()
	fmt.Println()

	fmt.Printf("  %s Download complete\n\n", greenC("✓"))

	mbPerSec := 0.0
	if totalDur.Seconds() > 0 {
		mbPerSec = float64(dlStats.TotalBytes) / (1024 * 1024) / totalDur.Seconds()
	}

	printKV("Chunks", fmt.Sprintf("%d/%d %s",
		dlStats.SuccessCount, dlStats.TotalChunks, dimC("verified")))
	printKV("Downloaded", formatBytes(dlStats.TotalBytes))
	printKV("Total Time", boldC(formatDuration(totalDur)))
	printKV("Throughput", fmt.Sprintf("%s %s",
		magC(fmt.Sprintf("%.2f MB/s", mbPerSec)), dimC("(avg)")))
	printKV("Output", boldC(outPath))
	fmt.Println()

	return nil
}

// ──────────────────────────────────────────────────────────────────────
// relay — start relay server (renamed from "receive")
// ──────────────────────────────────────────────────────────────────────

func runRelay(cmd *cobra.Command, args []string) error {
	port, _ := cmd.Flags().GetInt("port")
	srv := receiver.New(port)
	return srv.Start()
}

// ──────────────────────────────────────────────────────────────────────
// Pretty-print helpers
// ──────────────────────────────────────────────────────────────────────

func banner() string {
	return fmt.Sprintf("\n  %s  %s\n  %s\n\n",
		cyanC("▲"), boldC("Aether"),
		dimC("High-Performance File Transfer Engine"),
	)
}

func printHeader()                              { fmt.Print(banner()) }
func printStep(msg string)                      { fmt.Printf("  %s %s\n", cyanC(">>"), msg) }
func printSuccess(f string, a ...interface{})   { fmt.Printf("  %s %s\n", greenC("✓"), fmt.Sprintf(f, a...)) }
func printError(f string, a ...interface{})     { fmt.Printf("  %s %s\n", redC("✗"), fmt.Sprintf(f, a...)) }
func printDivider()                             { fmt.Printf("  %s\n", dimC("────────────────────────────────────────")) }

func printKV(key, value string) {
	padding := 14 - len(key)
	if padding < 1 {
		padding = 1
	}
	fmt.Printf("  %s%s%s\n", dimC(key), strings.Repeat(" ", padding), value)
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
