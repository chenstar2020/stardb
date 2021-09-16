package stardb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	_ "google.golang.org/genproto/googleapis/cloud/accessapproval/v1"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"stardb/index"
	"stardb/storage"
	"stardb/utils"
	"sync"
	"time"
)

var (
	ErrEmptyKey = errors.New("stardb: the key is empty")
	ErrKeyNotExist = errors.New("stardb: key not exist")
	ErrKeyTooLarge = errors.New("stardb: key exceeded the max length")
	ErrValueTooLarge = errors.New("stardb: value exceeded the max length")
	ErrNilIndexer = errors.New("stardb: indexer is nil")
	ErrCfgNotExist = errors.New("stardb: the config file not exist")
	ErrReclaimUnreached = errors.New("stardb: unused space not reach the threshold")
	ErrExtraContainsSeparator = errors.New("stardb: extra contains separator \\0")
	ErrInvalidTTL = errors.New("stardb: invalid ttl")
	ErrKeyExpired = errors.New("stardb: key is expired")
	ErrDBisReclaiming = errors.New("stardb: can't do reclaim and single reclaim at the same time")
)

const (
	configSaveFile = string(os.PathSeparator) + "DB.CFG"   //db配置文件

	dbMetaSaveFile = string(os.PathSeparator) + "DB.META"  //db文件偏移量

	reclaimPath = string(os.PathSeparator) + "stardb_reclaim" //文件回收创建的临时目录

	ExtraSeparator = "\\0"

	DataStructureNum = 5       //五种类型数据结构体数量
)

type  (
	StarDB struct {
		activeFile 				ActiveFiles   //活跃文件
		activeFileIds 			ActiveFileIds //活跃文件id
		archFiles  				ArchivedFiles //已归档文件
		strIndex				*StrIdx      //String index
		listIndex				*ListIdx     //List   index
		hashIndex				*HashIdx     //Hash   index
		setIndex				*SetIdx      //Set    index
		zsetIndex               *ZsetIdx     //ZSet   index
		config 					Config
		mu 						sync.RWMutex
		meta					*storage.DBMeta
		expires                 Expires      //过期目录
		isReclaiming            bool
		isSingleReclaiming      bool
	}

	// ActiveFiles 当前活跃文件
	ActiveFiles map[DataType]*storage.DBFile

	// ActiveFileIds 当前活跃文件id
	ActiveFileIds map[DataType]uint32

	// ArchivedFiles 已存档文件（只读）
	ArchivedFiles map[DataType]map[uint32]*storage.DBFile

	// Expires 过期信息
	Expires map[DataType]map[string]int64
)

// Open 打开一个stardb实例
func Open(config Config) (*StarDB, error){
	if !utils.Exist(config.DirPath){
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	archFiles, activeFileIds, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	//加载活跃文件
	activeFiles := make(ActiveFiles)
	for dataType, fileId := range activeFileIds {
		file, err := storage.NewDBFile(config.DirPath, fileId, config.RwMethod, config.BlockSize, dataType)
		if err != nil {
			return nil, err
		}
		activeFiles[dataType] = file
	}

	//加载db meta  得到活跃文件的偏移量
	meta := storage.LoadMeta(config.DirPath + dbMetaSaveFile)
	for dataType, file := range activeFiles{
		file.Offset = meta.ActiveWriteOff[dataType]
	}

	db := &StarDB{
		activeFile: activeFiles,
		activeFileIds: activeFileIds,
		archFiles: archFiles,
		config: config,
		strIndex: newStrIdx(),
		meta: meta,
		listIndex: newListIdx(),
		hashIndex: newHashIdx(),
		setIndex: newSetIdx(),
		zsetIndex: newZsetIdx(),
		expires: make(Expires),
	}

	for i := 0; i < DataStructureNum; i++ {
		db.expires[uint16(i)] = make(map[string]int64)
	}

	//加载索引
	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

func Reopen(path string)(*StarDB, error){
	if exist := utils.Exist(path + configSaveFile); !exist{
		return nil, ErrCfgNotExist
	}

	var config Config
	b, err := ioutil.ReadFile(path + configSaveFile)
	if err != nil{
		return nil, err
	}
	if err := json.Unmarshal(b, &config); err != nil{
		return nil, err
	}

	return Open(config)
}


func (db *StarDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.saveConfig(); err != nil{
		return err
	}

	if err := db.saveMeta(); err != nil{
		return err
	}

	for _, file := range db.activeFile{
		if err := file.Close(true); err != nil{
			return err
		}
	}
	//关闭已归档的文件
	for _, archFile := range db.archFiles{
		for _, file := range archFile{
			if err := file.Sync(); err != nil{
				return err
			}
		}
	}

	return nil
}

func (db *StarDB) Sync() error {
	if db == nil || db.activeFile == nil{
		return nil
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, file := range db.activeFile{
		if err := file.Sync(); err != nil{
			return err
		}
	}
	return nil
}
/*
 *回收db文件
 */
func (db *StarDB) Reclaim()(err error){
	if db.isSingleReclaiming{
		return ErrDBisReclaiming
	}
	var reclaimable bool
	for _, archFiles := range db.archFiles{
		if len(archFiles) >= db.config.ReclaimThreshold{ //只要有一种数据类型达到文件回收门槛
			reclaimable = true
			break
		}
	}
	if !reclaimable{
		return ErrReclaimUnreached
	}

	reclaimPath := db.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil{
		return err
	}
	defer os.RemoveAll(reclaimPath)

	db.mu.Lock()
	defer func() {
		db.isReclaiming = false
		db.mu.Unlock()
	}()
	db.isReclaiming = true

	newArchivedFiles := sync.Map{}
	reclaimedTypes := sync.Map{}
	wg := sync.WaitGroup{}
	wg.Add(DataStructureNum)
	for i := 0; i < DataStructureNum; i++{
		go func(dType uint16){
			defer func() {
				wg.Done()
			}()

			if len(db.archFiles[dType]) < db.config.ReclaimThreshold{
				newArchivedFiles.Store(dType, db.archFiles[dType])
				return
			}

			var (
				df *storage.DBFile
				fileId uint32
				archFiles = make(map[uint32]*storage.DBFile)
				fileIds []int
			)

			for _, file := range db.archFiles[dType]{
				fileIds = append(fileIds, int(file.Id))
			}
			sort.Ints(fileIds)

			for _, fid := range fileIds{
				file := db.archFiles[dType][uint32(fid)]
				var offset int64
				var reclaimEntries []*storage.Entry

				for{
					if e, err := file.Read(offset); err == nil{
						if db.validEntry(e, offset, file.Id){
							reclaimEntries = append(reclaimEntries, e)
						}
						offset += int64(e.Size())
					}else{
						if err == io.EOF{
							break
						}
						log.Fatalf("err occured when read the entry:%+v", err)
						return
					}
				}

				for _, entry := range reclaimEntries{
					if df == nil || int64(entry.Size()) + df.Offset > db.config.BlockSize{
						df, err = storage.NewDBFile(reclaimPath, fileId, db.config.RwMethod, db.config.BlockSize, dType)
						if err != nil{
							log.Fatalf("err occurred when create new db file:%+v", err)
							return
						}
						archFiles[fileId] = df
						fileId += 1
					}

					if err = df.Write(entry); err != nil{
						log.Fatalf("err occurred when write the entry:%+v", err)
						return
					}

					if dType == String {
						item := db.strIndex.idxList.Get(entry.Meta.Key)
						idx := item.Value().(*index.Indexer)
						idx.Offset = df.Offset - int64(entry.Size())
						idx.FileId = fileId
						db.strIndex.idxList.Put(idx.Meta.Key, idx)
					}
				}
			}
			reclaimedTypes.Store(dType, struct{}{})
			newArchivedFiles.Store(dType, archFiles)
		}(uint16(i))
	}
	wg.Wait()

	dbArchivedFiles := make(ArchivedFiles)
	for i := 0; i < DataStructureNum; i++{
		dType := uint16(i)
		value, ok := newArchivedFiles.Load(dType)
		if !ok{
			log.Printf("one type of data(%d) is missed after reclaiming", dType)
			return
		}
		dbArchivedFiles[dType] = value.(map[uint32]*storage.DBFile)
	}

	for dataType, files := range db.archFiles{
		if _, exist := reclaimedTypes.Load(dataType); exist{
			for _, f := range files{
				_ = os.Remove(f.File.Name())
			}
		}
	}

	for dataType, files := range dbArchivedFiles{
		if _, exist := reclaimedTypes.Load(dataType); exist{
			for _, f := range files{
				name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[dataType], f.Id)
				os.Rename(reclaimPath + name, db.config.DirPath + name)
			}
		}
	}

	db.archFiles = dbArchivedFiles
	return
}

func (db *StarDB) SingleReclaim()(err error){
	if db.isReclaiming{
		return ErrDBisReclaiming
	}

	reclaimPath := db.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(reclaimPath)

	db.mu.Lock()
	defer func() {
		db.isSingleReclaiming = false
		db.mu.Unlock()
	}()

	db.isSingleReclaiming = true
	var fileIds []int
	for _, file := range db.archFiles[String]{
		fileIds = append(fileIds, int(file.Id))
	}
	sort.Ints(fileIds)

	for _, fid := range fileIds{
		file := db.archFiles[String][uint32(fid)]
		if db.meta.ReclaimableSpace[file.Id] < db.config.SingleReclaimThreshold{
			continue
		}

		var(
			readOff int64
			validEntries []*storage.Entry
		)

		for{
			entry, err := file.Read(readOff)
			if err != nil{
				if err == io.EOF{
					break
				}
				return err
			}
			if db.validEntry(entry, readOff, uint32(fid)){
				validEntries = append(validEntries, entry)
			}
			readOff += int64(entry.Size())
		}

		if len(validEntries) == 0{
			os.Remove(file.File.Name())
			delete(db.meta.ReclaimableSpace, uint32(fid))
			delete(db.archFiles[String], uint32(fid))
			continue
		}

		df, err := storage.NewDBFile(reclaimPath, uint32(fid), db.config.RwMethod, db.config.BlockSize, String)
		if err != nil{
			return err
		}
		for _, e := range validEntries{
			if err := df.Write(e); err != nil{
				return err
			}

			item := db.strIndex.idxList.Get(e.Meta.Key)
			idx := item.Value().(*index.Indexer)
			idx.Offset = df.Offset - int64(e.Size())
			idx.FileId = uint32(fid)
			db.strIndex.idxList.Put(idx.Meta.Key, idx)
		}

		os.Remove(file.File.Name())

		name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[String], fid)
		os.Rename(reclaimPath + name, db.config.DirPath)

		db.meta.ReclaimableSpace[uint32(fid)] = 0
		db.archFiles[String][uint32(fid)] = df
	}


	return
}

func (db *StarDB) Backup(dir string)(err error){
	if utils.Exist(db.config.DirPath){
		err = utils.CopyDir(db.config.DirPath, dir)
	}
	return
}

//load String/List/Hash/Set/ZSet indexes
func (db *StarDB) loadIdxFromFiles()(err error) {
	if db.archFiles == nil && db.activeFile == nil {
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(DataStructureNum)
	for dataType := 0; dataType < DataStructureNum; dataType++ {
		go func(dType uint16){
			defer func() {
				wg.Done()
			}()

			var fileIds []int
			dbFile := make(map[uint32]*storage.DBFile)
			for k, v := range db.archFiles[dType]{
				dbFile[k] = v
				fileIds = append(fileIds, int(k))
			}

			//active file
			dbFile[db.activeFileIds[dType]] = db.activeFile[dType]
			fileIds = append(fileIds, int(db.activeFileIds[dType]))

			sort.Ints(fileIds)
			for i := 0; i < len(fileIds); i++ {
				fid := uint32(fileIds[i])
				df := dbFile[fid]
				var offset int64 = 0

				for offset <= db.config.BlockSize {
					if e, err := df.Read(offset); err == nil {
						idx := &index.Indexer{
							Meta: 		e.Meta,
							FileId: 	fid,
							EntrySize: 	e.Size(),
							Offset: 	offset,
						}
						offset += int64(e.Size())
						//根据entry重建索引  将每个entry都执行一遍
						if len(e.Meta.Key) > 0 {
							if err := db.buildIndex(e, idx); err != nil{
								log.Fatalf("a fatal error occured, the db can't open:[%v]", err)
							}
						}
					}else{
						if err == io.EOF{
							break
						}
						log.Fatalf("a fatal err occured, , the db can't open:[%v]", err)
					}
				}
			}

		}(uint16(dataType))
	}

	wg.Wait()
	return nil
}

func (db *StarDB) checkKeyValue(key []byte, value ...[]byte) error{
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}

	config := db.config
	if keySize > config.MaxKeySize{
		return ErrKeyTooLarge
	}

	for _, v := range value{
		if uint32(len(v)) > config.MaxValueSize{
			return ErrValueTooLarge
		}
	}
	return nil
}

/*
 *保存db配置
 */
func (db *StarDB) saveConfig()(err error){
	path := db.config.DirPath + configSaveFile
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)  //O_TRUNC 文件截断为0 会清空文件

	b, err := json.Marshal(db.config)
	_, err = file.Write(b)
	err = file.Close()

	return
}

func (db *StarDB) saveMeta() error{
	metaPath := db.config.DirPath + dbMetaSaveFile
	return db.meta.Store(metaPath)
}

func (db *StarDB)buildIndex(entry *storage.Entry, idx *index.Indexer) error {
	//key value都保存在内存中
	if db.config.IdxMode == KeyValueMemMode {
		idx.Meta.Value = entry.Meta.Value
		idx.Meta.ValueSize = uint32(len(entry.Meta.Value))
	}

	switch entry.GetType() {
	case storage.String:
		db.buildStringIndex(idx, entry)
	case storage.List:
		db.buildListIndex(idx, entry)
	case storage.Hash:
		db.buildHashIndex(idx, entry)
	case storage.Set:
		db.buildSetIndex(idx, entry)
	case storage.ZSet:
		db.buildZsetIndex(idx, entry)
	}
	return nil
}

//保存entry到db file
func (db *StarDB) store(e *storage.Entry) error{
	// 如果文件大小不够，刷新数据到磁盘  再打开一个新的文件
	config := db.config
	if db.activeFile[e.GetType()].Offset + int64(e.Size()) > config.BlockSize{
		if err := db.activeFile[e.GetType()].Sync(); err != nil{
			return err
		}

		activeFileId := db.activeFileIds[e.GetType()]
		db.archFiles[e.GetType()][activeFileId] = db.activeFile[e.GetType()]
		activeFileId = activeFileId + 1

		//打开一个新的db文件
		newDbFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize, e.GetType())
		if err != nil{
			return err
		}
		db.activeFile[e.GetType()] = newDbFile
		db.activeFileIds[e.GetType()] = activeFileId
		db.meta.ActiveWriteOff[e.GetType()] = 0
	}

	if err := db.activeFile[e.GetType()].Write(e); err != nil{
		return err
	}
	db.meta.ActiveWriteOff[e.GetType()] = db.activeFile[e.GetType()].Offset

	if config.Sync {
		if err := db.activeFile[e.GetType()].Sync(); err != nil{
			return err
		}
	}

	return nil
}

/*
 *校验entry的有效性
 */
func (db *StarDB)validEntry(e *storage.Entry, offset int64, fileId uint32) bool{
	if e == nil{
		return false
	}

	mark := e.GetMark()
	switch e.GetType(){
	case String:
		deadline, exist := db.expires[String][string(e.Meta.Key)]
		if !exist{
			return false
		}

		now := time.Now().Unix()

		if mark == StringExpire{
			if deadline > now {
				return true
			}
		}
		if mark == StringSet || mark == StringPersist{
			if deadline <= now {
				return false
			}

			node := db.strIndex.idxList.Get(e.Meta.Key)
			if node == nil{
				return false
			}
			indexer := node.Value().(*index.Indexer)
			if bytes.Compare(indexer.Meta.Key, e.Meta.Key) == 0{
				if indexer != nil && indexer.FileId == fileId && indexer.Offset == offset{
					return true
				}
			}
		}
	case List:
		if mark == ListLExpire{
			deadline, exist := db.expires[List][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix(){
				return true
			}
		}
		if mark == ListLPush || mark == ListRPush || mark == ListLInsert || mark == ListLSet{
			if db.LValExists(e.Meta.Key, e.Meta.Value){
				return true
			}
		}
	case Hash:
		if mark == HashHExpire{
			deadline, exist := db.expires[Hash][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix(){
				return true
			}
			if mark == HashHSet {
				if val := db.HGet(e.Meta.Key, e.Meta.Extra); string(val) == string(e.Meta.Value){
					return true
				}
			}
		}
	case Set:
		if mark == SetSExpire{
			deadline, exist := db.expires[Set][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix(){
				return true
			}
		}
		if mark == SetSMove{
			if db.SIsMember(e.Meta.Extra, e.Meta.Value){
				return true
			}
		}
		if mark == SetSAdd{
			if db.SIsMember(e.Meta.Key, e.Meta.Value){
				return true
			}
		}
	case ZSet:
		if mark == ZSetZExpire{
			deadline, exist := db.expires[ZSet][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix(){
				return true
			}
		}
		if mark == ZSetZAdd{
			if val, err := utils.StrToFloat64(string(e.Meta.Extra)); err == nil{
				score := db.ZScore(e.Meta.Key, e.Meta.Value)
				if score == val{
					return true
				}
			}
		}
	}

	return false
}

func (db *StarDB)checkExpired(key []byte, dType DataType)(expired bool){
	deadline, exist := db.expires[dType][string(key)]
	if !exist{
		return
	}

	if time.Now().Unix() > deadline{
		expired = true

		var e *storage.Entry
		switch dType{
		case String:
			e = storage.NewEntryNoExtra(key, nil, String, StringRem)
			if ele := db.strIndex.idxList.Remove(key); ele != nil{
				db.incrReclaimableSpace(key)
			}
		case List:
			e = storage.NewEntryNoExtra(key, nil, List, ListLClear)
			db.listIndex.indexes.LClear(string(key))
		case Hash:
			e = storage.NewEntryNoExtra(key, nil, Hash, HashHClear)
			db.hashIndex.indexes.HClear(string(key))
		case Set:
			e = storage.NewEntryNoExtra(key, nil, Set, SetSClear)
			db.setIndex.indexes.SClear(string(key))
		case ZSet:
			e = storage.NewEntryNoExtra(key, nil, List, ListLClear)
			db.zsetIndex.indexes.ZClear(string(key))
		}
		if err := db.store(e); err != nil{
			log.Println("checkExpired: store entry err: ", err)
			return
		}

		delete(db.expires[dType], string(key))
	}
	return
}