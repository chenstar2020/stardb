package index

import (
	"stardb/storage"
)

type Indexer struct {
	Meta 		*storage.Meta
	//FileId、EntrySize、Offset用于Get string时从磁盘加载数据
	FileId 		uint32
	EntrySize 	uint32
	Offset    	int64
}