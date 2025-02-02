package ds

import "github.com/emirpasic/gods/trees/redblacktree"

type RedBlackTreeMemTable struct {
	tree *redblacktree.Tree
}

// NewRedBlackTreeMemTable creates and initializes a new MemTable
func NewRedBlackTreeMemTable() *RedBlackTreeMemTable {
	t := redblacktree.NewWithStringComparator()

	return &RedBlackTreeMemTable{
		tree: t,
	}
}

func (r *RedBlackTreeMemTable) Set(key string, value interface{}) {
	r.tree.Put(key, value)
}

func (r *RedBlackTreeMemTable) Get(key string) (interface{}, bool) {
	return r.tree.Get(key)
}

func (r *RedBlackTreeMemTable) Len() int64 {
	return int64(r.tree.Size())
}
