package stardb

import (
	"stardb/ds/set"
	"sync"
	"stardb/storage"
	"time"
)

type SetIdx struct {
	mu	sync.RWMutex
	indexes *set.Set
}

func newSetIdx() *SetIdx{
	return &SetIdx{indexes: set.New()}
}

func (db *StarDB) SAdd(key []byte, members ...[]byte)(res int, err error){
	if err = db.checkKeyValue(key, members...); err != nil{
		return
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	for _, m := range members{
		exist := db.setIndex.indexes.SIsMember(string(key), m)
		if !exist{
			e := storage.NewEntryNoExtra(key, m, Set, SetSAdd)
			if err = db.store(e); err != nil{
				res = db.setIndex.indexes.SAdd(string(key), m)
			}
		}
	}
	return
}

func (db *StarDB) SPop(key []byte, count int)(values [][]byte, err error){
	if err = db.checkKeyValue(key, nil); err != nil{
		return
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(key, Set){
		return nil, ErrKeyExpired
	}

	values = db.setIndex.indexes.SPop(string(key), count)
	for _, v := range values{
		e := storage.NewEntryNoExtra(key, v, Set, SetSRem)
		if err = db.store(e); err != nil{
			return
		}
	}
	return
}

func (db *StarDB) SIsMember(key, member []byte) bool{
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set){
		return false
	}

	return db.setIndex.indexes.SIsMember(string(key), member)
}

func (db *StarDB) SRandMember(key []byte, count int) [][]byte{
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set){
		return nil
	}
	return db.setIndex.indexes.SRandMember(string(key), count)
}

func (db *StarDB) SRem(key []byte, members ...[]byte)(res int, err error){
	if err = db.checkKeyValue(key, members...); err != nil{
		return
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(key, Set){
		return
	}

	for _, m := range members{
		if ok := db.setIndex.indexes.SRem(string(key), m);ok{
			e := storage.NewEntryNoExtra(key, m, Set, SetSRem)
			if err = db.store(e); err != nil{
				return
			}
			res++
		}
	}
	return
}

func (db *StarDB) SMove(src, dst, member []byte) error{
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(src, Set){
		return ErrKeyExpired
	}
	if db.checkExpired(dst, Set){
		return ErrKeyExpired
	}

	if ok := db.setIndex.indexes.SMove(string(src), string(dst), member);ok{
		e := storage.NewEntry(src, member, dst, Set, SetSMove)
		if err := db.store(e); err != nil{
			return err
		}
	}
	return nil
}

func (db *StarDB)SCard(key []byte) int{
	if err := db.checkKeyValue(key, nil); err != nil{
		return 0
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set){
		return 0
	}

	return db.setIndex.indexes.SCard(string(key))
}

func (db *StarDB)SMembers(key []byte) (val [][]byte){
	if err := db.checkKeyValue(key, nil); err != nil{
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set){
		return
	}

	return db.setIndex.indexes.SMembers(string(key))
}

func (db *StarDB)SUnion(keys ...[]byte)(val [][]byte){
	if keys == nil || len(keys) == 0{
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	var validKeys []string
	for _, k := range keys{
		if db.checkExpired(k, Set){
			continue
		}
		validKeys = append(validKeys, string(k))
	}

	return db.setIndex.indexes.SUnion(validKeys...)
}

func (db *StarDB)SDiff(keys ...[]byte)(val [][]byte){
	if keys == nil || len(keys) == 0{
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	var validKeys []string
	for _, k := range keys{
		if db.checkExpired(k, Set){
			continue
		}
		validKeys = append(validKeys, string(k))
	}

	return db.setIndex.indexes.SDiff(validKeys...)
}

func (db *StarDB) SKeyExists(key []byte)(ok bool){
	if err := db.checkKeyValue(key, nil); err != nil{
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set){
		return
	}

	ok = db.setIndex.indexes.SKeyExists(string(key))
	return
}

func (db *StarDB) SClear(key []byte)(err error){
	if !db.SKeyExists(key){
		return ErrKeyNotExist
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, nil, Set, SetSClear)
	if err = db.store(e); err != nil{
		return
	}
	db.setIndex.indexes.SClear(string(key))
	return
}

func (db *StarDB) SExpire(key []byte, duration int64)(err error){
	if duration <= 0{
		return ErrInvalidTTL
	}

	if !db.SKeyExists(key){
		return ErrKeyExpired
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(key, nil, deadline, Set, SetSExpire)
	if err = db.store(e); err != nil{
		return
	}
	db.expires[Set][string(key)] = deadline
	return
}

func (db *StarDB) STTL(key []byte)(ttl int64){
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set){
		return
	}
	deadline, exist := db.expires[Set][string(key)]
	if !exist{
		return
	}

	return deadline - time.Now().Unix()
}