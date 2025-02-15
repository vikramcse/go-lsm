package sstable

import (
	"bytes"
	"encoding/binary"
)

// IBlock represents the index block of an SSTable.
// The index block contains sorted entries that map keys to data block locations.
// Each entry contains:
// - The first key in the data block
// - The block handle (offset and size) for that data block
type IBlock struct {
	entries []IndexEntry
}

// IndexEntry represents a single entry in the index block.
// It maps the first key of a data block to that block's location in the file.
type IndexEntry struct {
	Key         []byte      // First key in the referenced data block
	BlockHandle BlockHandle // Location and size of the referenced data block
}

func NewIBlock() *IBlock {
	return &IBlock{
		entries: make([]IndexEntry, 0),
	}
}

// AddEntry adds a new entry to the index block.
// Called when a data block is flushed to disk.
func (ib *IBlock) AddEntry(key []byte, handle BlockHandle) {
	ib.entries = append(ib.entries, IndexEntry{
		Key:         key,
		BlockHandle: handle,
	})
}

// Encode serializes the index block to bytes in the following format:
// - Number of entries (uint32)
// For each entry:
// - Key length (uint32)
// - Key bytes
// - Block offset (uint64)
// - Block size (uint64)
func (ib *IBlock) Encode() []byte {
	buf := new(bytes.Buffer)

	// Write number of entries
	binary.Write(buf, binary.LittleEndian, uint32(len(ib.entries)))

	// Write each entry
	for _, entry := range ib.entries {
		// Write key length and key
		binary.Write(buf, binary.LittleEndian, uint32(len(entry.Key)))
		buf.Write(entry.Key)

		// Write block handle
		binary.Write(buf, binary.LittleEndian, entry.BlockHandle.Offset)
		binary.Write(buf, binary.LittleEndian, entry.BlockHandle.Size)
	}

	return buf.Bytes()
}
