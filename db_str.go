package stardb

import (
	"stardb/index"
	"bytes"
	"strings"
	"sync"
	"stardb/storage"
	"time"
)

// StrIdx 字符串索引
type StrIdx struct {
	mu		sync.RWMutex
	idxList *index.SkipList
}

func newStrIdx()*StrIdx{
	return &StrIdx{idxList: index.NewSkipList()}
}

func (db *StarDB)Set(key, value []byte) error{
	return db.doSet(key, value)
}

func (db *StarDB)SetNx(key, value []byte)(res uint32, err error){
	if exist := db.StrExists(key); exist{
		return
	}

	if err = db.Set(key, value); err == nil{
		res = 1
	}
	return
}

func (db *StarDB)Get(key []byte)([]byte, error){
	if err := db.checkKeyValue(key, nil); err != nil{
		return nil, err
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	node := db.strIndex.idxList.Get(key)
	if node == nil{
		return nil, ErrKeyNotExist
	}

	idx := node.Value().(*index.Indexer)
	if idx == nil{
		return nil, ErrNilIndexer
	}

	if db.checkExpired(key, String){
		return nil, ErrKeyExpired
	}

	if db.config.IdxMode == KeyValueMemMode {
		return idx.Meta.Value, nil
	}

	if db.config.IdxMode == KeyOnlyMemMode {
		df := db.activeFile[String]

		if idx.FileId != db.activeFileIds[String]{
			df = db.archFiles[String][idx.FileId]
		}

		e, err := df.Read(idx.Offset)
		if err != nil{
			return nil, err
		}
		return e.Meta.Value, nil
	}
	return nil, ErrKeyNotExist
}

func (db *StarDB) GetSet(key, val []byte)(res []byte, err error){
	res, err = db.Get(key)
	if err != nil && err != ErrKeyNotExist{
		return
	}
	if err = db.Set(key, val); err != nil{
		return
	}
	return
}

func (db *StarDB) Append(key, value []byte) error{
	if err := db.checkKeyValue(key, value); err != nil{
		return err
	}
	existVal, err := db.Get(key)
	if err != nil && err != ErrKeyNotExist && err != ErrKeyExpired{
		return err
	}

	if len(existVal) > 0 {
		existVal = append(existVal, value...)
	}else{
		existVal = value
	}
	return db.doSet(key, existVal)
}

func (db *StarDB) StrLen(key []byte) int{
	if err := db.checkKeyValue(key, nil); err != nil{
		return 0
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	e := db.strIndex.idxList.Get(key)
	if e != nil {
		if db.checkExpired(key, String){
			return 0
		}
		idx := e.Value().(*index.Indexer)
		return int(idx.Meta.ValueSize)
	}
	return 0
}

func (db *StarDB) StrExists(key []byte) bool{
	if err := db.checkKeyValue(key, nil); err != nil{
		return false
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	exist := db.strIndex.idxList.Exist(key)
	if exist && !db.checkExpired(key, String){
		return true
	}

	return false
}

func (db *StarDB) StrRem(key []byte) error {
	if err := db.checkKeyValue(key, nil); err != nil{
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, nil, String, StringRem)
	if err := db.store(e); err != nil{
		return err
	}

	db.incrReclaimableSpace(key)
	db.strIndex.idxList.Remove(key)
	delete(db.expires[String], string(key))
	return nil
}

func (db *StarDB) PrefixScan(prefix string, limit, offset int)(val [][]byte, err error){
	if limit == 0{
		return
	}
	if offset < 0 {
		offset = 0
	}
	if err = db.checkKeyValue([]byte(prefix), nil); err != nil{
		return
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := db.strIndex.idxList.FindPrefix([]byte(prefix))
	if limit > 0 {
		for i := 0; i < offset && e != nil && strings.HasPrefix(string(e.Key()), prefix);i++{
			e = e.Next()
		}
	}

	for e != nil && strings.HasPrefix(string(e.Key()), prefix) && limit != 0{
		item := e.Value().(*index.Indexer)
		var value []byte

		if db.config.IdxMode == KeyOnlyMemMode{
			value, err = db.Get(e.Key())
			if err != nil{
				return
			}
		}else{
			if item != nil{
				value = item.Meta.Value
			}
		}

		expired := db.checkExpired(e.Key(), String)
		if !expired{
			val = append(val, value)
			e = e.Next()
		}
		if limit > 0 && !expired{
			limit--
		}
	}
	return
}

func (db *StarDB) RangeScan(start, end []byte)(val [][]byte, err error){
	node := db.strIndex.idxList.Get(start)

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	for node != nil && bytes.Compare(node.Key(), end) <= 0{
		if db.checkExpired(node.Key(), String){
			node = node.Next()
			continue
		}

		var value []byte
		if db.config.IdxMode == KeyOnlyMemMode {
			value, err = db.Get(node.Key())
			if err != nil && err != ErrKeyNotExist{
				return nil, err
			}
		}else{
			value = node.Value().(*index.Indexer).Meta.Value
		}

		val = append(val, value)
		node = node.Next()
	}
	return
}

func (db *StarDB)Expire(key []byte, duration int64)(err error){
	if duration <= 0{
		return ErrInvalidTTL
	}
	if !db.StrExists(key){
		return ErrKeyNotExist
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration

	e := storage.NewEntryWithExpire(key, nil, deadline, String, StringExpire)
	if err = db.store(e); err != nil{
		return err
	}

	db.expires[String][string(key)] = deadline
	return
}

func (db *StarDB) Persist(key []byte)(err error){
	val, err := db.Get(key)
	if err != nil{
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, val, String, StringPersist)
	if err = db.store(e); err != nil{
		return
	}

	delete(db.expires[String], string(key))
	return
}

func (db *StarDB) TTL(key []byte)(ttl int64){
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	deadline, exist := db.expires[String][string(key)]
	if !exist{
		return
	}
	if expired := db.checkExpired(key, String); expired{
		return
	}
	return deadline - time.Now().Unix()
}

//增加可回收空间
func (db *StarDB) incrReclaimableSpace(key []byte){
	oldIdx := db.strIndex.idxList.Get(key)
	if oldIdx != nil {
		indexer := oldIdx.Value().(*index.Indexer)

		if indexer != nil {
			space := int64(indexer.EntrySize)
			db.meta.ReclaimableSpace[indexer.FileId] += space
		}
	}
}

func (db *StarDB) doSet(key, value []byte)(err error){
	if err = db.checkKeyValue(key, value); err != nil{
		return
	}

	if db.config.IdxMode == KeyValueMemMode{
		if existVal, _ := db.Get(key); existVal != nil && bytes.Compare(existVal, value) == 0{
			return
		}
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, value, String, StringSet)
	if err := db.store(e); err != nil{
		return err
	}

	db.incrReclaimableSpace(key)

	if _, ok := db.expires[String][string(key)]; ok{
		delete(db.expires[String], string(key))
	}

	idx := &index.Indexer{
		Meta: &storage.Meta{
			KeySize: uint32(len(e.Meta.Key)),
			Key: e.Meta.Key,
			ValueSize: uint32(len(e.Meta.Value)),
		},
		FileId: db.activeFileIds[String],
		EntrySize: e.Size(),
		Offset: db.activeFile[String].Offset - int64(e.Size()),
	}

	if db.config.IdxMode == KeyValueMemMode{
		idx.Meta.Value = e.Meta.Value
	}
	db.strIndex.idxList.Put(idx.Meta.Key, idx)
	return
}