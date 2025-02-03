package sstable

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"

	golsm "github.com/vikramcse/go-lsm"
)

// Writer handles writing SSTable files
type Writer struct {
	file      *os.File
	block     *Block
	bufWriter *bufio.Writer
	filename  string
	offset    uint64
}

// NewWriter creates a new SSTable writer
func NewWriter(dir string) (*Writer, error) {
	file_name := golsm.SSTableFilePrefix + time.Now().Format("20060102150405") + ".sst"
	full_file_name := filepath.Join(dir, file_name)

	file, err := os.OpenFile(full_file_name, os.O_CREATE|os.O_WRONLY, 0664)
	if err != nil {
		return nil, err
	}

	return &Writer{
		file:      file,
		bufWriter: bufio.NewWriter(file),
		filename:  full_file_name,
		block:     NewBlock(),
	}, nil
}

// Write adds a key-value pair to the SSTable
func (w *Writer) Write(key string, value []byte) error {
	if w.block.IsFull() {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	w.block.AddEntry([]byte(key), value)
	return nil
}

// flushBlock writes the current block to disk
func (w *Writer) flushBlock() error {
	if w.block.IsEmpty() {
		return nil
	}

	// Encode the data
	data := w.block.Encode()

	// Create a metadata for Data Block
	metadata := &BlockMetadata{
		Type:     DataBlock,
		CRC:      crc32.ChecksumIEEE(data),
		Size:     uint32(len(data)),
		KeyCount: uint32(w.block.KeyCount()),
	}

	// write the metadata
	if err := binary.Write(w.bufWriter, binary.LittleEndian, metadata); err != nil {
		return err
	}

	// write the actual data
	if _, err := w.bufWriter.Write(data); err != nil {
		return err
	}

	// add the new offset
	w.offset += uint64(len(data)) + uint64(binary.Size(metadata))

	// as this block is flused, create a new one
	w.block = NewBlock()
	return nil
}

// Close finalizes the SSTable file
func (w *Writer) Close() error {
	// Flush any remaining data
	if err := w.flushBlock(); err != nil {
		return err
	}

	// Flush buffer
	if err := w.bufWriter.Flush(); err != nil {
		return err
	}

	return w.file.Close()
}
