package storage

import (
	"fmt"
	"testing"
)

func TestDBMeta_Store(t *testing.T) {
	path := "D:\\github\\stardb\\testFile\\test.Meta"
	writeOff := make(map[uint16]int64)
	writeOff[0] = 34
	reclaimableSpace := make(map[uint32]int64)
	m := &DBMeta{
		ActiveWriteOff: writeOff,
		ReclaimableSpace: reclaimableSpace,
	}
	err := m.Store(path)
	if err != nil{
		t.Error("store file err:", err)
	}
}

func TestLoadMeta(t *testing.T) {
	path := "D:\\github\\stardb\\testFile\\test.Meta"
	meta := LoadMeta(path)
	fmt.Printf("%+v", meta)
}
