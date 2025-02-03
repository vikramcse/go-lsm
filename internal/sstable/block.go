package sstable

import (
	"bytes"
	"encoding/binary"
)

// Entry is a key-value pair in a block
type Entry struct {
	Key   []byte
	Value []byte
}

// Block represents a data block in sstable
type Block struct {
	entries []Entry
	size    uint32
}

// NewBlock creates a new block
func NewBlock() *Block {
	return &Block{
		entries: make([]Entry, 0),
	}
}

// AddEntry adds a new entry to the block
func (b *Block) AddEntry(key, value []byte) {
	b.entries = append(b.entries, Entry{
		Key:   key,
		Value: value,
	})

	b.size += uint32(len(key) + len(value))
}

// IsFull checks if block has reached its size limit
func (b *Block) IsFull() bool {
	return b.size >= BlockSize
}

// IsEmpty checks if block has reached its size limit
func (b *Block) IsEmpty() bool {
	return len(b.entries) == 0
}

// KeyCount checks if block has reached its size limit
func (b *Block) KeyCount() int {
	return len(b.entries)
}

// Encode serializes the Block into a byte buffer. The serialized format includes:
// - The number of entries in the block (as a uint32).
// - For each entry:
//   - The length of the key (as a uint32).
//   - The key itself (as a byte slice).
//   - The length of the value (as a uint32).
//   - The value itself (as a byte slice).
//
// Example:
// If the Block contains the following entries:
//
//	entries := []Entry{
//	    {Key: []byte("key1"), Value: []byte("value1")},
//	    {Key: []byte("key2"), Value: []byte("value2")},
//	}
//
// The encoded byte buffer will be structured as follows:
//
//	[number of entries (2)][key1 length (4)][key1 ("key1")][value1 length (6)][value1 ("value1")]
//	[key2 length (4)][key2 ("key2")][value2 length (6)][value2 ("value2")]
//
// Lengths of keys and values are written using binary.LittleEndian to ensure
// consistent and correct interpretation across different systems. This is
// important for interoperability and efficient reading. The actual key and
// value data are written as raw bytes, as byte order does not apply to them.
func (b *Block) Encode() []byte {
	buf := new(bytes.Buffer)

	// Write number of entries
	binary.Write(buf, binary.LittleEndian, uint32(len(b.entries)))

	// Write each entry
	for _, entry := range b.entries {
		// Write key length
		binary.Write(buf, binary.LittleEndian, uint32(len(entry.Key)))
		// Write key
		buf.Write(entry.Key)

		// Write value length
		binary.Write(buf, binary.LittleEndian, uint32(len(entry.Value)))
		// Write value
		buf.Write(entry.Value)
	}

	return buf.Bytes()
}
