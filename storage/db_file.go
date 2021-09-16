package storage

import (
	_ "encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/roseduan/mmap-go"
	_ "go/types"
	"hash/crc32"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	// FilePerm 默认的创建文件权限
	FilePerm = 0644

	PathSeparator = string(os.PathSeparator)
)

var (
	DBFileFormatNames = map[uint16]string{
		0: "%09d.data.str",
		1: "%09d.data.list",
		2: "%09d.data.hash",
		3: "%09d.data.set",
		4: "%09d.data.zset",
	}

	DBFileSuffixName = []string{"str", "list", "hash", "set", "zset"}
)

var (
	// the entry is empty
	ErrEmptyEntry = errors.New("storage/db_file: entry or the Key of entry is empty")
)

// FileRWMethod 文件数据读写方式
type FileRWMethod uint8

const (
	// FileIO 使用系统标准IO
	FileIO FileRWMethod = iota
	// MMap 使用内存映射
	MMap
)

// DBFile stardb数据文件定义
type DBFile struct {
	Id uint32
	path string
	File *os.File
	mmap mmap.MMap
	Offset int64
	method FileRWMethod
}

// NewDBFile 新建一个数据读写文件， 如果是MMap, 则需要Truncate文件并进行加载
func NewDBFile(path string, fileId uint32, method FileRWMethod, blockSize int64, eType uint16)(*DBFile, error){
	filePath := path + PathSeparator + fmt.Sprintf(DBFileFormatNames[eType], fileId)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil{
		return nil, err
	}

	df := &DBFile{Id: fileId, path: path, Offset: 0, method: method}

	if method == FileIO {
		df.File = file
	} else {
		if err = file.Truncate(blockSize); err != nil {   //Truncate 修改文件大小
			return nil, err
		}
		m, err := mmap.Map(file, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		df.mmap = m
	}
	return df, nil
}

//从数据文件读数据， offset是读的起始位置
func (df *DBFile)Read(offset int64)(e *Entry,  err error){
	var buf []byte
	//读出头部信息 (crc, keysize, valuesize, extrasize, state, timestamp)
	if buf, err = df.readBuf(offset, int64(entryHeaderSize)); err != nil{
		return
	}
    //头部信息解码
	if e, err = Decode(buf); err != nil {
		return
	}
    //读key
	offset += entryHeaderSize
	if e.Meta.KeySize > 0 {
		var key []byte
		if key, err = df.readBuf(offset, int64(e.Meta.KeySize)); err != nil {
			return
		}
		e.Meta.Key = key
	}
    //读value
	offset += int64(e.Meta.KeySize)
	if e.Meta.ValueSize > 0 {
		var val []byte
		if val, err = df.readBuf(offset, int64(e.Meta.ValueSize)); err != nil {
			return
		}
		e.Meta.Value = val
	}
    //读extra
	offset += int64(e.Meta.ValueSize)
	if e.Meta.ExtraSize > 0 {
		var val []byte
		if val, err = df.readBuf(offset, int64(e.Meta.ExtraSize)); err != nil {
			return
		}
		e.Meta.Extra = val
	}
    //校验crc
	checkCrc := crc32.ChecksumIEEE(e.Meta.Value)
	if checkCrc != e.crc32 {
		return nil, ErrInvalidCrc
	}

	return
}

//从文件的offset处开始写数据
func (df *DBFile) Write(e *Entry) error {
	if e == nil || e.Meta.KeySize == 0{
		return ErrEmptyEntry
	}

	method := df.method
	writeOff := df.Offset
	encVal, err := e.Encode()
	if err != nil{
		return err
	}

	if method == FileIO{
		if _, err := df.File.WriteAt(encVal, writeOff); err != nil{
			return err
		}
	}
	if method == MMap{
		copy(df.mmap[writeOff:], encVal)
	}
	df.Offset += int64(e.Size())
	return nil
}

func (df *DBFile) readBuf(offset int64, n int64)([]byte, error) {
	buf := make([]byte, n)

	if df.method == FileIO{
		_, err := df.File.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}
	}

	if df.method == MMap && offset <= int64(len(df.mmap)) {
		copy(buf, df.mmap[offset:])
	}

	return buf, nil
}

// Build 加载数据文件
func Build(path string, method FileRWMethod, blockSize int64)(map[uint16]map[uint32]*DBFile, map[uint16]uint32, error){
	dir, err := ioutil.ReadDir(path)  //读取目录下的所有文件
	if err != nil{
		return nil, nil, err
	}

	fileIdsMap := make(map[uint16][]int)  //存储不同文件类型id集合
	for _, d := range dir {
		if strings.Contains(d.Name(), ".data"){
			splitNames := strings.Split(d.Name(), ".")
			id, _ := strconv.Atoi(splitNames[0])

			switch splitNames[2] {
			case DBFileSuffixName[0]:
				fileIdsMap[0] = append(fileIdsMap[0], id)
			case DBFileSuffixName[1]:
				fileIdsMap[1] = append(fileIdsMap[1], id)
			case DBFileSuffixName[2]:
				fileIdsMap[2] = append(fileIdsMap[2], id)
			case DBFileSuffixName[3]:
				fileIdsMap[3] = append(fileIdsMap[3], id)
			case DBFileSuffixName[4]:
				fileIdsMap[4] = append(fileIdsMap[4], id)
			}
		}
	}

	activeFileIds := make(map[uint16]uint32)     //map[dataType]fileID
	archFiles := make(map[uint16]map[uint32]*DBFile)  //map[dataType]map[fileID]*DBFile
	var dataType uint16 = 0
	for ; dataType < 5; dataType++ {
		fileIDs := fileIdsMap[dataType]
		sort.Ints(fileIDs)
		files := make(map[uint32]*DBFile)  //保存需要创建的文件句柄
		var activeFileId uint32 = 0

		if len(fileIDs) > 0 {
			activeFileId = uint32(fileIDs[len(fileIDs) - 1])  //最后一个id文件为活跃文件 最后一个文件没写满，前面都已经写满

			for i := 0; i < len(fileIDs) - 1; i++ {
				id := fileIDs[i]

				file, err := NewDBFile(path, uint32(id), method, blockSize, dataType)
				if err != nil {
					return nil, nil, err
				}
				files[uint32(id)] = file
			}
		}
		archFiles[dataType] = files
		activeFileIds[dataType] = activeFileId
	}
	return archFiles, activeFileIds, nil
}

//关闭文件
func (df *DBFile) Close(sync bool) (err error){
	if sync {  //关闭前是否持久化文件
		err = df.Sync()
	}

	if df.File != nil{
		err = df.File.Close()
	}

	if df.mmap != nil{
		err = df.mmap.Unmap()
	}
	return
}

//数据持久化
func (df *DBFile) Sync() (err error){
	if df.File != nil{
		err = df.File.Sync()  //将文件系统的最近写入的数据在内存中的拷贝刷新到硬盘中稳定保存
	}
	if df.mmap != nil{
		err = df.mmap.Flush()
	}
	return
}