package storage

import (
	"fmt"
	"os"
	"testing"
	"time"
)

const (
	path1 = "D:\\github\\stardb\\testFile"
	fileID1 = 0
	path2 = ""
	defaultBlockSize = 8 * 1024 * 1024
)

func init(){
/*	os.MkdirAll(path1, os.ModePerm)
	filePath := path1 + PathSeparator + fmt.Sprintf("%09d.data.str", 0)

	_, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil{
		log.Println("create file fail, err:", err)
		return
	}
	filePath = path1 + PathSeparator + fmt.Sprintf("%09d.data.list", 0)
	os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm)*/
}

func TestNewDBFile(t *testing.T) {
	os.MkdirAll(path1, os.ModePerm)
	newOne := func(method FileRWMethod, dataType uint16){
		_, err := NewDBFile(path1, fileID1, method, defaultBlockSize, dataType)
		if err != nil{
			t.Error("new db file error", err)
		}
	}

	t.Run("new db file io", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			newOne(FileIO, uint16(i))
		}
	})

	t.Run("new db file mmap", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			newOne(MMap, uint16(i))
		}
	})

}

func TestDBFile_Sync(t *testing.T) {
	df, err :=  NewDBFile(path1, fileID1, FileIO, defaultBlockSize, 1)
	if err != nil{
		t.Error(err)
	}
	df.Sync()
}

func TestDBFile_Close(t *testing.T) {
	df, err :=  NewDBFile(path1, fileID1, FileIO, defaultBlockSize, 3)
	if err != nil{
		t.Error(err)
	}
	df.Close(true)
}

func TestDBFile_Write(t *testing.T) {
	df, err :=  NewDBFile(path1, 1, FileIO, defaultBlockSize, 3)
	if err != nil{
		t.Error(err)
	}
	defer df.Close(true)

	entry1 := &Entry{
		Meta: &Meta{
			Key: []byte("testkey001"),
			Value: []byte("testvalue001"),
		},
	}
	entry1.Timestamp = uint64(time.Now().Unix())
	entry1.Meta.KeySize = uint32(len(entry1.Meta.Key))
	entry1.Meta.ValueSize = uint32(len(entry1.Meta.Value))

	entry2 := &Entry{
		Meta: &Meta{
			Key: []byte("testkey002"),
			Value: []byte("testvalue002"),
		},
	}
	entry2.Timestamp = uint64(time.Now().Unix())
	entry2.Meta.KeySize = uint32(len(entry2.Meta.Key))
	entry2.Meta.ValueSize = uint32(len(entry2.Meta.Value))
	df.Write(entry1)
	df.Write(entry2)

	entry, err := df.Read(int64(entry1.Size()))
	if err != nil{
		t.Error("read entry fail", err)
	}
	fmt.Println("entry:", string(entry.Meta.Key), string(entry.Meta.Value))
}

func TestBuild(t *testing.T) {
	archFile, activeFile, err := Build(path1, FileIO, defaultBlockSize)
	if err != nil{
		t.Error("build file fail", err)
	}
	fmt.Println(len(archFile[3]), activeFile[3])
}

