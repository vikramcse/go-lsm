// Package sstable implements a Sorted String Table (SSTable) format for storing key-value pairs.
// SSTable is an immutable, ordered file format that stores key-value pairs sorted by key.
// The format consists of data blocks, an index block, and a footer.

package sstable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

// BlockType represents different types of blocks in SSTable.
// An SSTable file contains two types of blocks:
// - Data blocks: Store actual key-value pairs
// - Index blocks: Store index entries pointing to data blocks
type BlockType uint8

const (
	DataBlock BlockType = iota
	IndexBlock
)

// CompressionType represents the compression algorithm used
type CompressionType uint8

const (
	NoCompression CompressionType = iota
	SnappyCompression
	LZ4Compression
)

// BlockMetadata contains metadata for each block in the SSTable.
// This metadata is stored before each block in the file and includes:
// - Type: Whether it's a data block or index block
// - CRC: Checksum for data integrity verification
// - Size: Size of the block data in bytes
// - KeyCount: Number of key-value pairs in the block
// - Compressed: Whether the block is compressed
type BlockMetadata struct {
	Type       BlockType
	CRC        uint32
	Size       uint32
	KeyCount   uint32
	Compressed bool
}

// BlockHandle stores the location and size of a block in the SSTable file.
// Used by the index block to point to data blocks, and by the footer to
// point to the index block.
type BlockHandle struct {
	Offset uint64 // Position of the block in the file
	Size   uint64 // Size of the block in bytes
}

// Footer contains metadata about the entire SSTable file.
// The footer is stored at the end of the file and has a fixed size.
// It includes:
// - IndexHandle: Location of the index block
// - FilterHandle: Location of the bloom filter block (if present)
// - MagicNumber: For file format verification
// - Version: SSTable format version
// - CreatedAt: Timestamp when the file was created
// - CompressionType: Compression algorithm used (if any)
type Footer struct {
	IndexHandle     BlockHandle
	FilterHandle    BlockHandle
	MagicNumber     uint64
	Version         uint32
	CreatedAt       int64 // Changed from time.Time to int64 (Unix timestamp)
	CompressionType CompressionType
}

// EncodeFooter serializes the footer to bytes
func (f *Footer) Encode() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, f.IndexHandle)
	binary.Write(buf, binary.LittleEndian, f.FilterHandle)
	binary.Write(buf, binary.LittleEndian, f.MagicNumber)
	binary.Write(buf, binary.LittleEndian, f.Version)
	binary.Write(buf, binary.LittleEndian, f.CreatedAt)
	binary.Write(buf, binary.LittleEndian, f.CompressionType)
	return buf.Bytes()
}

// DecodeFooter deserializes bytes into a Footer
func DecodeFooter(data []byte) (*Footer, error) {
	buf := bytes.NewReader(data)
	footer := &Footer{}

	if err := binary.Read(buf, binary.LittleEndian, &footer.IndexHandle); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &footer.FilterHandle); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &footer.MagicNumber); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &footer.Version); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &footer.CreatedAt); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &footer.CompressionType); err != nil {
		return nil, err
	}

	return footer, nil
}

const (
	// Various constants for SSTable
	MagicNumber    = 0x8773537461626c65 // "SSTable" in hex
	CurrentVersion = 1
	BlockSize      = 4 * 1024 // 4KB default block size
	FooterSize     = 64       // BlockHandle (16) + BlockHandle (16) + uint64 (8) + uint32 (4) + int64 (8) + uint8 (1) = 53, padded to 64
)

// calculateCRC calculates CRC32 checksum for data
func calculateCRC(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
