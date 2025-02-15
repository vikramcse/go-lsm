package sstable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

// BlockType represents different types of blocks in SSTable
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

// BlockMetadata contains metadata for each block
type BlockMetadata struct {
	Type       BlockType
	CRC        uint32
	Size       uint32
	KeyCount   uint32
	Compressed bool
}

// BlockHandle stores the location and size of a block
type BlockHandle struct {
	Offset uint64
	Size   uint64
}

// Footer contains metadata about the SSTable
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
