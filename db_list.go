package stardb

import (
	"bytes"
	"stardb/ds/list"
	"stardb/storage"
	"strconv"
	"strings"
	"sync"
)

// ListIdx list 索引
type ListIdx struct {
	mu 		 sync.RWMutex
	indexes  *list.List
}

func newListIdx() *ListIdx{
	return &ListIdx{indexes: list.New()}
}

func (db *StarDB) LPush(key []byte, values ...[]byte)(res int, err error){
	if err = db.checkKeyValue(key, values...); err != nil{
		return
	}
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, List, ListLPush)
		if err = db.store(e); err != nil{
			return
		}

		res = db.listIndex.indexes.LPush(string(key), val)
	}
	return
}

func (db *StarDB) RPush(key []byte, values ...[]byte)(res int, err error){
	if err = db.checkKeyValue(key, values...); err != nil{
		return
	}
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, List, ListRPush)
		if err = db.store(e); err != nil{
			return
		}

		res = db.listIndex.indexes.RPush(string(key), val)
	}
	return
}

func (db *StarDB) LPop(key []byte)(val []byte, err error) {
	if err = db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.listIndex.mu.Lock()
	db.listIndex.mu.Unlock()

	if db.checkExpired(key, List){
		return nil, ErrKeyExpired
	}

	val = db.listIndex.indexes.LPop(string(key))
	if val != nil{
		e := storage.NewEntryNoExtra(key, val, List, ListLPop)
		if err := db.store(e); err != nil{
			return nil, err
		}
	}
	return val, nil
}

func (db *StarDB) RPop(key []byte)(val []byte, err error) {
	if err = db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.listIndex.mu.Lock()
	db.listIndex.mu.Unlock()

	if db.checkExpired(key, List){
		return nil, ErrKeyExpired
	}

	val = db.listIndex.indexes.RPop(string(key))
	if val != nil{
		e := storage.NewEntryNoExtra(key, val, List, ListRPop)
		if err := db.store(e); err != nil{
			return nil, err
		}
	}
	return val, nil
}

func (db *StarDB) LIndex(key []byte, idx int)(val []byte){
	if err := db.checkKeyValue(key);err != nil{
		return nil
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	return db.listIndex.indexes.LIndex(string(key), idx)
}

func (db *StarDB) LRem(key, value []byte, count int)(int, error){
	if err := db.checkKeyValue(key, value); err != nil{
		return 0, nil
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(key, List){
		return 0, ErrKeyExpired
	}

	res := db.listIndex.indexes.LRem(string(key), value, count)
	if res > 0{
		c := strconv.Itoa(count)
		e := storage.NewEntry(key, value, []byte(c), List, ListLRem)
		if err := db.store(e); err != nil{
			return res, err
		}
	}
	return res, nil
}

func (db *StarDB) LInsert(key []byte, option list.InsertOption, pivot, val []byte)(count int, err error){
	if err = db.checkKeyValue(key, val); err != nil{
		return
	}

	if strings.Contains(string(pivot), ExtraSeparator){
		return 0, ErrExtraContainsSeparator
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	count = db.listIndex.indexes.LInsert(string(key), option, pivot, val)
	if count != -1{
		var buf bytes.Buffer
		buf.Write(pivot)
		buf.Write([]byte(ExtraSeparator))
		opt := strconv.Itoa(int(option))
		buf.Write([]byte(opt))

		e := storage.NewEntry(key, val, buf.Bytes(), List, ListLInsert)
		if err = db.store(e); err != nil{
			return
		}
	}
	return
}

func (db *StarDB) LSet(key []byte, idx int, val []byte)(ok bool, err error){
	if err := db.checkKeyValue(key, val); err != nil{
		return false, err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if ok = db.listIndex.indexes.LSet(string(key), idx, val); ok{
		i := strconv.Itoa(idx)
		e := storage.NewEntry(key, val, []byte(i), List, ListLSet)
		if err := db.store(e); err != nil{
			return false, err
		}
	}
	return
}

func (db *StarDB) LTrim(key []byte, start, end int)(err error){
	if err = db.checkKeyValue(key, nil); err != nil{
		return
	}
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(key, List){
		return ErrKeyExpired
	}

	if res := db.listIndex.indexes.LTrim(string(key), start, end); res{
		var buf bytes.Buffer
		buf.Write([]byte(strconv.Itoa(start)))
		buf.Write([]byte(ExtraSeparator))
		buf.Write([]byte(strconv.Itoa(end)))

		e := storage.NewEntry(key, nil, buf.Bytes(), List, ListLTrim)
		if err := db.store(e); err != nil{
			return err
		}
	}
	return nil
}

func (db *StarDB) LRange(key []byte, start, end int)(val [][]byte, err error){
	if err = db.checkKeyValue(key, nil); err != nil{
		return
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	return db.listIndex.indexes.LRange(string(key), start, end), nil
}

func (db *StarDB) LLen(key []byte) int{
	if err := db.checkKeyValue(key, nil); err != nil{
		return 0
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	return db.listIndex.indexes.LLen(string(key))
}

func (db *StarDB) LKeyExists(key []byte) (ok bool){
	if err := db.checkKeyValue(key, nil); err != nil{
		return
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.checkExpired(key, List){
		return false
	}

	ok = db.listIndex.indexes.LKeyExists(string(key))
	return
}

func (db *StarDB) LValExists(key, val []byte)(ok bool){
	if err := db.checkKeyValue(key, nil); err != nil{
		return
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.checkExpired(key, List){
		return false
	}

	ok = db.listIndex.indexes.LValExists(string(key), val)
	return
}