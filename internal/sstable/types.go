package sstable

// BlockType represents different types of blocks in SSTable
type BlockType uint8

const (
	DataBlock BlockType = iota
	IndexBlock
)

// BlockMetadata contains metadata for each block
type BlockMetadata struct {
	Type     BlockType
	CRC      uint32
	Size     uint32
	KeyCount uint32
}

const BlockSize = 4 * 1024 // 4KB default block size
