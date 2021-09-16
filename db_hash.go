package stardb

import (
	"stardb/ds/hash"
	"bytes"
	"sync"
	"stardb/storage"
)

type HashIdx struct {
	mu 		sync.RWMutex
	indexes *hash.Hash
}

func newHashIdx() *HashIdx{
	return &HashIdx{indexes: hash.New()}
}

func (db *StarDB) HSet(key []byte, field []byte, value []byte)(res int, err error){
	if err = db.checkKeyValue(key, value); err != nil{
		return
	}
	//值没有发生变化
	oldVal := db.HGet(key, field)
	if bytes.Compare(oldVal, value) == 0{
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	e := storage.NewEntry(key, value, field, Hash, HashHSet)
	if err = db.store(e); err != nil{
		return
	}

	res = db.hashIndex.indexes.HSet(string(key), string(field), value)
	return
}

func (db *StarDB) HSetNx(key, field, value []byte)(res int, err error){
	if err = db.checkKeyValue(key, value); err != nil{
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if res = db.hashIndex.indexes.HSetNx(string(key), string(field), value); res == 1{
		e := storage.NewEntry(key, value, field, Hash, HashHSet)
		if err = db.store(e); err != nil{
			return
		}
	}
	return
}

func (db *StarDB) HGet(key, field []byte) []byte{
	if err := db.checkKeyValue(key, nil); err != nil{
		return nil
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, Hash){
		return nil
	}

	return db.hashIndex.indexes.HGet(string(key), string(field))
}

func (db *StarDB) HGetAll(key []byte)[][]byte{
	if err := db.checkKeyValue(key, nil); err != nil{
		return nil
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, Hash){
		return nil
	}

	return db.hashIndex.indexes.HGetAll(string(key))
}

func (db *StarDB) HDel(key []byte, field ...[]byte)(res int, err error){
	if err = db.checkKeyValue(key, nil); err != nil{
		return
	}

	if field == nil || len(field) == 0 {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	for _, f := range field {
		if ok := db.hashIndex.indexes.HDel(string(key), string(f));ok == 1{
			e := storage.NewEntry(key, nil, f, Hash, HashHDel)
			if err = db.store(e); err != nil{
				return
			}
			res++
		}
	}
	return
}

func (db *StarDB) HExists(key, filed []byte) int{
	if err := db.checkKeyValue(key, nil); err != nil{
		return 0
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, Hash){
		return 0
	}

	return db.hashIndex.indexes.HExist(string(key), string(filed))
}

func (db *StarDB) HLen(key []byte) int{
	if err := db.checkKeyValue(key, nil); err != nil{
		return 0
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, Hash){
		return 0
	}

	return db.hashIndex.indexes.HLen(string(key))
}

func (db *StarDB) HKeys(key []byte) (val []string){
	if err := db.checkKeyValue(key, nil); err != nil{
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, Hash){
		return
	}

	return db.hashIndex.indexes.HKeys(string(key))
}

func (db *StarDB) HVals(key []byte) (val [][]byte){
	if err := db.checkKeyValue(key, nil); err != nil{
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, Hash){
		return
	}

	return db.hashIndex.indexes.HVals(string(key))
}