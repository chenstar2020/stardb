package stardb

import (
	"math"
	"stardb/ds/zset"
	"stardb/storage"
	"stardb/utils"
	"sync"
	"time"
)

type ZsetIdx struct {
	mu 		sync.RWMutex
	indexes *zset.SortedSet
}

func newZsetIdx() *ZsetIdx{
	return &ZsetIdx{indexes: zset.New()}
}

func (db *StarDB) ZAdd(key []byte, score float64, member []byte)error{
	if err := db.checkKeyValue(key, member); err != nil{
		return err
	}

	if oldScore := db.ZScore(key, member); oldScore == score{
		return nil
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	extra := []byte(utils.Float64ToStr(score))
	e := storage.NewEntry(key, member, extra, ZSet, ZSetZAdd)
	if err := db.store(e); err != nil{
		return err
	}

	db.zsetIndex.indexes.ZAdd(string(key), score, string(member))
	return nil
}

func (db *StarDB) ZCard(key []byte) int {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet){
		return 0
	}

	return db.zsetIndex.indexes.ZCard(string(key))
}

func (db *StarDB) ZScore(key, member []byte) float64{
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet){
		return math.MinInt64
	}

	return db.zsetIndex.indexes.ZScore(string(key), string(member))
}

func (db *StarDB) ZRank(key, member []byte) int64{
	if err := db.checkKeyValue(key, member); err != nil{
		return -1
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet){
		return -1
	}

	return db.zsetIndex.indexes.ZRank(string(key), string(member))
}


func (db *StarDB) ZRevRank(key, member []byte) int64{
	if err := db.checkKeyValue(key, member); err != nil {
		return -1
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return -1
	}

	return db.zsetIndex.indexes.ZRevRank(string(key), string(member))
}

func (db *StarDB) ZIncrBy(key []byte, increment float64, member[]byte)(float64, error){
	if err := db.checkKeyValue(key, member); err != nil{
		return increment, err
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	increment = db.zsetIndex.indexes.ZIncrBy(string(key), increment, string(member))

	extra := utils.Float64ToStr(increment)
	e := storage.NewEntry(key, member, []byte(extra), ZSet, ZSetZAdd)
	if err := db.store(e); err != nil{
		return increment, err
	}

	return increment, nil
}

func (db *StarDB) ZRange(key []byte, start, stop int)[]interface{}{
	if err := db.checkKeyValue(key, nil); err != nil{
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRange(string(key), start, stop)
}

func (db *StarDB) ZRangeWithScores(key []byte, start, stop int)[]interface{}{
	if err := db.checkKeyValue(key, nil); err != nil{
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRangeWithScores(string(key), start, stop)
}

func (db *StarDB) ZRevRange(key []byte, start, stop int)[]interface{}{
	if err := db.checkKeyValue(key, nil); err != nil{
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevRange(string(key), start, stop)
}

func (db *StarDB) ZRevRangeWithScores(key []byte, start, stop int)[]interface{}{
	if err := db.checkKeyValue(key, nil); err != nil{
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevRangeWithScores(string(key), start, stop)
}

func (db *StarDB) ZRem(key, member []byte)(ok bool, err error){
	if err := db.checkKeyValue(key, nil); err != nil{
		return false, err
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()


	if  db.zsetIndex.indexes.ZRem(string(key), string(member)){
		e := storage.NewEntryNoExtra(key, member, ZSet, ZSetZRem)
		if err = db.store(e); err != nil{
			return
		}
	}

	return
}

func (db *StarDB) ZGetByRank(key []byte, rank int)[]interface{}{
	if err := db.checkKeyValue(key, nil); err != nil{
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet){
		return nil
	}

	return db.zsetIndex.indexes.ZGetByRank(string(key), rank)
}

func (db *StarDB) ZRevGetByRank(key []byte, rank int)[]interface{}{
	if err := db.checkKeyValue(key, nil); err != nil{
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet){
		return nil
	}

	return db.zsetIndex.indexes.ZRevGetByRank(string(key), rank)
}

func (db *StarDB) ZScoreRange(key []byte, min, max float64)[]interface{}{
	if err := db.checkKeyValue(key, nil); err != nil{
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet){
		return nil
	}

	return db.zsetIndex.indexes.ZScoreRange(string(key), min, max)
}

func (db *StarDB) ZRevScoreRange(key []byte, max, min float64)[]interface{}{
	if err := db.checkKeyValue(key, nil); err != nil{
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet){
		return nil
	}

	return db.zsetIndex.indexes.ZRevScoreRange(string(key), max, min)
}

func (db *StarDB) ZKeyExists(key []byte)(ok bool){
	if err := db.checkKeyValue(key, nil); err != nil{
		return
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(key, ZSet){
		return
	}

	return db.zsetIndex.indexes.ZKeyExists(string(key))
}

func (db *StarDB) ZClear(key []byte)(err error){
	if err := db.checkKeyValue(key, nil); err != nil{
		return
	}

	if !db.ZKeyExists(key){
		err = ErrKeyNotExist
		return
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	e := storage.NewEntryNoExtra(key, nil, ZSet, ZSetZClear)
	if err = db.store(e); err != nil{
		return
	}

	db.zsetIndex.indexes.ZClear(string(key))
	return
}

func (db *StarDB) ZExpire(key []byte, duration int64)(err error){
	if duration <= 0{
		return ErrInvalidTTL
	}
	if !db.ZKeyExists(key){
		return ErrKeyNotExist
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(key, nil, deadline, ZSet, ZSetZExpire)
	if err = db.store(e); err != nil{
		return
	}

	db.expires[ZSet][string(key)] = deadline
	return
}

//return time to live of key
func (db *StarDB) ZTTL(key []byte)(ttl int64){
	if !db.ZKeyExists(key){
		return
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	deadline, exist := db.expires[ZSet][string(key)]
	if !exist{
		return
	}

	return deadline - time.Now().Unix()
}