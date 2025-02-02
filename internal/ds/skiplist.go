package ds

import (
	"github.com/huandu/skiplist"
)

type SkipListMemTable struct {
	list *skiplist.SkipList
}

// NewSkipListMemTable creates and initializes a new MemTable
func NewSkipListMemTable() *SkipListMemTable {
	l := skiplist.New(skiplist.String)

	return &SkipListMemTable{
		list: l,
	}
}

func (s *SkipListMemTable) Set(key string, value interface{}) {
	s.list.Set(key, value)
}

func (s *SkipListMemTable) Get(key string) (interface{}, bool) {
	return s.list.GetValue(key)
}

func (s *SkipListMemTable) Len() int64 {
	return int64(s.list.Len())
}
