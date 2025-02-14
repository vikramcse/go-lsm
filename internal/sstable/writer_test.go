package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"testing"
)

type testEntry struct {
	key   string
	value string
}

type testEntryBytes struct {
	key   string
	value []byte
}

func TestWriterBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp(".", "sstable_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewWriter(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Test data
	testData := []testEntry{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	// Write test data
	for _, td := range testData {
		err = writer.Write(td.key, []byte(td.value))
		if err != nil {
			t.Fatalf("Failed to write entry %s: %v", td.key, err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Verify file exists
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}

	if len(files) != 1 {
		t.Errorf("Expected 1 file, got %d", len(files))
	}

	verifyFileContent(t, writer.filename, testData)

}

func TestWriterBlockBoundry(t *testing.T) {
	tmpDir, err := os.MkdirTemp(".", "sstable_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewWriter(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	largeValue := make([]byte, BlockSize-100)
	for i := range largeValue {
		largeValue[i] = 'v'
	}

	testCases := []testEntryBytes{
		{"key1", largeValue},
		{"key2", []byte("small value")},
		{"key3", largeValue},
		{"emptyKey", []byte{}},
	}

	for _, tc := range testCases {
		err = writer.Write(tc.key, tc.value)
		if err != nil {
			t.Fatalf("Failed to write large entry: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}
}

func verifyFileContent(t *testing.T, filename string, expectedData []testEntry) {
	t.Helper()

	file, err := os.Open(filename)
	if err != nil {
		t.Fatalf("Failed to open file for verification: %v", err)
	}
	defer file.Close()

	// This line reads only the amount of data that corresponds to the
	// size of the BlockMetadata structure.
	var metadata BlockMetadata
	err = binary.Read(file, binary.LittleEndian, &metadata)
	if err != nil {
		t.Fatalf("Failed to read metadata: %v", err)
	}

	if metadata.Type != DataBlock {
		t.Errorf("Expected DataBlock type, got %v", metadata.Type)
	}

	// The file pointer is already positioned after the BlockMetadata
	// structure, so we can read the rest of the data.
	data := make([]byte, metadata.Size)
	_, err = file.Read(data)
	if err != nil {
		t.Fatalf("Failed to read block data: %v", err)
	}

	// Check the integrity by verifying CRC
	if crc32.ChecksumIEEE(data) != metadata.CRC {
		t.Error("CRC mismatch")
	}

	buf := bytes.NewBuffer(data)
	var entryCount uint32
	err = binary.Read(buf, binary.LittleEndian, &entryCount)
	if err != nil {
		t.Fatalf("Failed to read EntryCount data: %v", err)
	}

	if int(entryCount) != len(expectedData) {
		t.Errorf("Expected %d entries, got %d", len(expectedData), entryCount)
	}

	for i := 0; i < int(entryCount); i++ {
		var keyLen, valueLen uint32

		// read the keyLengh
		binary.Read(buf, binary.LittleEndian, &keyLen)
		// init a new byte with the size of keyLen
		key := make([]byte, keyLen)
		buf.Read(key)

		// read the valueLen
		binary.Read(buf, binary.LittleEndian, &valueLen)
		// init a new byte with the size of valueLen
		value := make([]byte, valueLen)
		buf.Read(value)

		if string(key) != expectedData[i].key {
			t.Errorf("Entry %d: expected key %s, got %s", i, expectedData[i].key, string(key))
		}
		if string(value) != expectedData[i].value {
			t.Errorf("Entry %d: expected value %s, got %s", i, expectedData[i].value, string(value))
		}
	}

}

// Benchmark writing
func BenchmarkWriter(b *testing.B) {
	tmpDir, err := os.MkdirTemp(".", "sstable_bench_*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewWriter(tmpDir)
	if err != nil {
		b.Fatalf("Failed to create writer: %v", err)
	}

	value := []byte("test value")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		if err := writer.Write(key, value); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		b.Fatalf("Failed to close writer: %v", err)
	}
}
