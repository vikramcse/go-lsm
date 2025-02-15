package main

import (
	"fmt"
	"log"
	"os"

	"github.com/vikramcse/go-lsm/internal/sstable"
)

func main() {
	// Create a temporary directory for the demo
	tmpDir, err := os.MkdirTemp(".", "sstable_demo_*")
	if err != nil {
		log.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Sample data to write
	testData := []struct {
		key   string
		value string
	}{
		{"apple", "red fruit"},
		{"banana", "yellow fruit"},
		{"cherry", "small red fruit"},
		{"date", "sweet dried fruit"},
		{"elderberry", "small black berry"},
	}

	// Create and write to SSTable
	fmt.Println("Writing data to SSTable...")
	sstPath := writeSSTable(tmpDir, testData)

	// Read and verify data
	fmt.Println("\nReading data from SSTable...")
	readSSTable(sstPath, testData)
}

func writeSSTable(dir string, data []struct {
	key   string
	value string
}) string {
	// Create a new SSTable writer
	writer, err := sstable.NewWriter(dir)
	if err != nil {
		log.Fatalf("Failed to create writer: %v", err)
	}

	// Write the test data
	for _, entry := range data {
		if err := writer.Write(entry.key, []byte(entry.value)); err != nil {
			log.Fatalf("Failed to write entry %s: %v", entry.key, err)
		}
		fmt.Printf("Wrote: %s -> %s\n", entry.key, entry.value)
	}

	// Close the writer
	if err := writer.Close(); err != nil {
		log.Fatalf("Failed to close writer: %v", err)
	}

	return writer.Filename()
}

func readSSTable(filename string, expectedData []struct {
	key   string
	value string
}) {
	// Create a new SSTable reader
	reader, err := sstable.NewReader(filename)
	if err != nil {
		log.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Read and verify each key-value pair
	for _, expected := range expectedData {
		value, err := reader.Get([]byte(expected.key))
		if err != nil {
			log.Fatalf("Failed to read key %s: %v", expected.key, err)
		}

		// Verify the value matches
		if string(value) != expected.value {
			log.Fatalf("Value mismatch for key %s: got %s, want %s",
				expected.key, string(value), expected.value)
		}

		fmt.Printf("Read: %s -> %s\n", expected.key, string(value))
	}
}
