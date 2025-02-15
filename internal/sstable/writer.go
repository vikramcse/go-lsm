// Package sstable provides functionality for creating and reading SSTable files.
// The Writer is responsible for creating new SSTable files and writing data in
// the correct format.

package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"

	golsm "github.com/vikramcse/go-lsm"
)

// Writer handles writing SSTable files. It manages:
// - Creating and writing data blocks
// - Building and writing the index block
// - Writing the footer
// - Managing block boundaries and file offsets
type Writer struct {
	file      *os.File      // The SSTable file being written
	block     *Block        // Current data block being built
	index     *IBlock       // Index block being built
	bufWriter *bufio.Writer // Buffered writer for better performance
	filename  string        // Name of the SSTable file
	offset    uint64        // Current offset in the file
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
		index:     NewIBlock(),
	}, nil
}

// Write adds a key-value pair to the SSTable.
// The process:
// 1. If current block is full, flush it to disk
// 2. Add the key-value pair to current block
// 3. Update index when blocks are flushed
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

	firstKey := w.block.entries[0].Key
	blockHandle := BlockHandle{
		Offset: w.offset,
		Size:   uint64(w.block.size),
	}
	w.index.AddEntry(firstKey, blockHandle)

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

// Close finalizes the SSTable file by:
// 1. Flushing any remaining data in the current block
// 2. Writing the index block
// 3. Writing the footer
// 4. Closing the file
func (w *Writer) Close() error {
	// Flush any remaining data
	if err := w.flushBlock(); err != nil {
		return err
	}

	// Store index block offset
	indexOffset := w.offset

	// Write the index block
	indexData := w.index.Encode()
	indexMetadata := &BlockMetadata{
		Type:     IndexBlock,
		CRC:      calculateCRC(indexData),
		Size:     uint32(len(indexData)),
		KeyCount: uint32(len(w.index.entries)),
	}

	// Write index metadata
	if err := binary.Write(w.bufWriter, binary.LittleEndian, indexMetadata); err != nil {
		return err
	}

	// Write index data
	if _, err := w.bufWriter.Write(indexData); err != nil {
		return err
	}

	// Update offset to include index block
	w.offset += uint64(len(indexData)) + uint64(binary.Size(indexMetadata))

	// Create and write footer
	footer := &Footer{
		IndexHandle:     BlockHandle{Offset: indexOffset, Size: uint64(len(indexData))},
		MagicNumber:     MagicNumber,
		Version:         CurrentVersion,
		CreatedAt:       time.Now().Unix(),
		CompressionType: NoCompression,
	}

	// Flush buffer before writing footer
	if err := w.bufWriter.Flush(); err != nil {
		return err
	}

	// Ensure footer is exactly FooterSize bytes
	footerData := footer.Encode()
	if len(footerData) > FooterSize {
		return errors.New("footer exceeds FooterSize")
	}

	// Pad footer data if necessary
	if len(footerData) < FooterSize {
		footerData = append(footerData, make([]byte, FooterSize-len(footerData))...)
	}

	if _, err := w.file.Write(footerData); err != nil {
		return err
	}

	return w.file.Close()
}

// Filename returns the name of the SSTable file
func (w *Writer) Filename() string {
	return w.filename
}
