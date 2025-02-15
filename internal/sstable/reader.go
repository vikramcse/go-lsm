package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

type Reader struct {
	file       *os.File
	indexBlock *IBlock
}

func NewReader(filename string) (*Reader, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	reader := &Reader{
		file: file,
	}

	// Read and validate the index block
	if err := reader.loadIndexBlock(); err != nil {
		file.Close()
		return nil, err
	}

	return reader, nil
}

// loadIndexBlock reads and loads the index block from the file
func (r *Reader) loadIndexBlock() error {
	// First seek to end of file minus footer size to read the footer
	fileInfo, err := r.file.Stat()
	if err != nil {
		return err
	}

	// Seek to footer position
	_, err = r.file.Seek(fileInfo.Size()-FooterSize, 0)
	if err != nil {
		return err
	}

	// Read footer
	footerData := make([]byte, FooterSize)
	if _, err := io.ReadFull(r.file, footerData); err != nil {
		return err
	}

	footer, err := DecodeFooter(footerData)
	if err != nil {
		return err
	}

	// Verify magic number
	if footer.MagicNumber != MagicNumber {
		return errors.New("invalid SSTable file: wrong magic number")
	}

	// Seek to index block position
	_, err = r.file.Seek(int64(footer.IndexHandle.Offset), 0)
	if err != nil {
		return err
	}

	// Read index block metadata
	var metadata BlockMetadata
	if err := binary.Read(r.file, binary.LittleEndian, &metadata); err != nil {
		return err
	}

	if metadata.Type != IndexBlock {
		return errors.New("invalid index block type")
	}

	// Read index block data
	data := make([]byte, metadata.Size)
	if _, err := io.ReadFull(r.file, data); err != nil {
		return err
	}

	// Verify CRC
	if calculateCRC(data) != metadata.CRC {
		return errors.New("index block CRC mismatch")
	}

	// Decode index block
	r.indexBlock = &IBlock{}
	if err := r.decodeIndexBlock(data); err != nil {
		return err
	}

	return nil
}

// decodeIndexBlock decodes the serialized index block data
func (r *Reader) decodeIndexBlock(data []byte) error {
	buf := bytes.NewReader(data)

	// Read number of entries
	var numEntries uint32
	if err := binary.Read(buf, binary.LittleEndian, &numEntries); err != nil {
		return err
	}

	r.indexBlock.entries = make([]IndexEntry, 0, numEntries)

	// Read each entry
	for i := uint32(0); i < numEntries; i++ {
		var keyLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return err
		}

		key := make([]byte, keyLen)
		if _, err := buf.Read(key); err != nil {
			return err
		}

		var handle BlockHandle
		if err := binary.Read(buf, binary.LittleEndian, &handle.Offset); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &handle.Size); err != nil {
			return err
		}

		r.indexBlock.entries = append(r.indexBlock.entries, IndexEntry{
			Key:         key,
			BlockHandle: handle,
		})
	}

	return nil
}

// Get retrieves the value for a given key
func (r *Reader) Get(key []byte) ([]byte, error) {
	if r.indexBlock == nil {
		return nil, errors.New("index block not loaded")
	}

	// Find the appropriate data block for the key
	blockHandle, err := r.findBlockHandle(key)
	if err != nil {
		return nil, err
	}

	// Read the block
	block, err := r.readBlock(blockHandle)
	if err != nil {
		return nil, err
	}

	// Search for the key in the block
	return r.searchInBlock(block, key)
}

// findBlockHandle finds the appropriate block handle for a given key
func (r *Reader) findBlockHandle(key []byte) (BlockHandle, error) {
	entries := r.indexBlock.entries

	// Binary search through index entries
	left, right := 0, len(entries)-1

	// If key is after last index entry, use last block
	if bytes.Compare(key, entries[right].Key) >= 0 {
		return entries[right].BlockHandle, nil
	}

	// Binary search for the block that may contain the key
	for left < right {
		mid := (left + right) / 2
		if bytes.Compare(entries[mid].Key, key) <= 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}

	// Use the block before the first block whose key is greater than search key
	if left > 0 {
		left--
	}
	return entries[left].BlockHandle, nil
}

// readBlock reads a data block from the file using the block handle
func (r *Reader) readBlock(handle BlockHandle) (*Block, error) {
	// Seek to the block position
	_, err := r.file.Seek(int64(handle.Offset), 0)
	if err != nil {
		return nil, err
	}

	// Read block metadata
	var metadata BlockMetadata
	if err := binary.Read(r.file, binary.LittleEndian, &metadata); err != nil {
		return nil, err
	}

	// Read block data
	data := make([]byte, metadata.Size)
	if _, err := io.ReadFull(r.file, data); err != nil {
		return nil, err
	}

	// Verify CRC
	if calculateCRC(data) != metadata.CRC {
		return nil, errors.New("block CRC mismatch")
	}

	// Decode the block data
	block := &Block{}
	buf := bytes.NewReader(data)

	// Read number of entries
	var numEntries uint32
	if err := binary.Read(buf, binary.LittleEndian, &numEntries); err != nil {
		return nil, err
	}

	block.entries = make([]Entry, 0, numEntries)

	// Read each entry
	for i := uint32(0); i < numEntries; i++ {
		var keyLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}

		key := make([]byte, keyLen)
		if _, err := buf.Read(key); err != nil {
			return nil, err
		}

		var valueLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &valueLen); err != nil {
			return nil, err
		}

		value := make([]byte, valueLen)
		if _, err := buf.Read(value); err != nil {
			return nil, err
		}

		block.entries = append(block.entries, Entry{
			Key:   key,
			Value: value,
		})
	}

	return block, nil
}

// searchInBlock searches for a key within a data block
func (r *Reader) searchInBlock(block *Block, key []byte) ([]byte, error) {
	// Binary search through block entries
	left, right := 0, len(block.entries)-1

	for left <= right {
		mid := (left + right) / 2
		cmp := bytes.Compare(block.entries[mid].Key, key)

		if cmp == 0 {
			return block.entries[mid].Value, nil
		} else if cmp < 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return nil, errors.New("key not found")
}

// Close closes the reader and its underlying file
func (r *Reader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
