package sstable

import (
	"bytes"
	"encoding/binary"
)

type IndexEntry struct {
	Key         []byte
	BlockHandle BlockHandle
}

type IBlock struct {
	entries []IndexEntry
}

func NewIBlock() *IBlock {
	return &IBlock{
		entries: make([]IndexEntry, 0),
	}
}

func (ib *IBlock) AddEntry(key []byte, handle BlockHandle) {
	ib.entries = append(ib.entries, IndexEntry{
		Key:         key,
		BlockHandle: handle,
	})
}

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
