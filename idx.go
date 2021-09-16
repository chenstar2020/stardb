package stardb

import (
	"stardb/ds/list"
	"stardb/index"
	"stardb/storage"
	"stardb/utils"
	"strconv"
	"strings"
	"time"
)

type DataType = uint16

//五种不同的数据类型
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

//字符串类型操作方式
const (
	StringSet  uint16 = iota      	//设置
	StringRem						//移除
	StringExpire					//过期
	StringPersist					//移动
)

//链表操作方式(这些操作会改变数据)
const (
	ListLPush uint16 = iota
	ListRPush
	ListLPop
	ListRPop
	ListLRem
	ListLInsert
	ListLSet
	ListLTrim
	ListLClear
	ListLExpire
)

const (
	HashHSet uint16 = iota
	HashHDel
	HashHClear
	HashHExpire
)

const (
	SetSAdd uint16 = iota
	SetSRem
	SetSMove
	SetSClear
	SetSExpire
)

const (
	ZSetZAdd uint16 = iota
	ZSetZRem
	ZSetZClear
	ZSetZExpire
)

func (db *StarDB) buildStringIndex(idx *index.Indexer, entry *storage.Entry){
	if db.strIndex == nil || idx == nil {
		return
	}

	switch entry.GetMark() {
	case StringSet:
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
	case StringRem:
		db.strIndex.idxList.Remove(idx.Meta.Key)
	case StringExpire:
		if entry.Timestamp < uint64(time.Now().Unix()){ //已过期的数据
			db.strIndex.idxList.Remove(idx.Meta.Key)
		}else{										    //设置过期时间
			db.expires[String][string(idx.Meta.Key)] = int64(entry.Timestamp)
		}
	case StringPersist:               //将过期数据移到跳表中
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
		delete(db.expires[String], string(idx.Meta.Key))
	}
}

func (db *StarDB) buildListIndex(idx *index.Indexer, entry *storage.Entry){
	if db.listIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch entry.GetMark(){
	case ListLPush:
		db.listIndex.indexes.LPush(key, idx.Meta.Value)
	case ListLPop:
		db.listIndex.indexes.LPop(key)
	case ListRPush:
		db.listIndex.indexes.RPush(key, idx.Meta.Value)
	case ListRPop:
		db.listIndex.indexes.RPop(key)
	case ListLRem:
		if count, err := strconv.Atoi(string(idx.Meta.Extra)); err == nil{
			db.listIndex.indexes.LRem(key, idx.Meta.Value, count)
		}
	case ListLInsert:
		extra := string(idx.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			pivot := []byte(s[0])
			if opt, err := strconv.Atoi(s[1]); err == nil {
				db.listIndex.indexes.LInsert(key, list.InsertOption(opt), pivot, idx.Meta.Value)
			}
		}
	case ListLSet:
		if i, err := strconv.Atoi(string(idx.Meta.Extra)); err == nil{
			db.listIndex.indexes.LSet(key, i, idx.Meta.Value)
		}
	case ListLTrim:
		extra := string(idx.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2{
			start, _ := strconv.Atoi(s[0])
			end, _ := strconv.Atoi(s[1])

			db.listIndex.indexes.LTrim(key, start, end)
		}
	case ListLExpire:
		if entry.Timestamp < uint64(time.Now().Unix()){
			db.listIndex.indexes.LClear(key)
		}else{
			db.expires[List][key] = int64(entry.Timestamp)
		}
	case ListLClear:
		db.listIndex.indexes.LClear(key)
	}
}

func (db *StarDB) buildHashIndex(idx *index.Indexer, entry *storage.Entry){
	if db.hashIndex == nil || idx == nil {
		return
	}

	key := string(idx.Meta.Key)
	switch entry.GetMark(){
	case HashHSet:
		db.hashIndex.indexes.HSet(key, string(idx.Meta.Extra), idx.Meta.Value)
	case HashHDel:
		db.hashIndex.indexes.HGet(key, string(idx.Meta.Extra))
	case HashHClear:
		db.hashIndex.indexes.HClear(key)
	case HashHExpire:
		if entry.Timestamp < uint64(time.Now().Unix()){
			db.hashIndex.indexes.HClear(key)
		} else {
			db.expires[Hash][key] = int64(entry.Timestamp)
		}
	}
}

func (db *StarDB) buildSetIndex(idx *index.Indexer, entry *storage.Entry){
	if db.hashIndex == nil || idx == nil{
		return
	}

	key := string(idx.Meta.Key)
	switch entry.GetMark(){
	case SetSAdd:
		db.setIndex.indexes.SAdd(key, idx.Meta.Value)
	case SetSRem:
		db.setIndex.indexes.SRem(key, idx.Meta.Value)
	case SetSMove:
		extra := idx.Meta.Extra
		db.setIndex.indexes.SMove(key, string(extra), idx.Meta.Value)
	case SetSClear:
		db.setIndex.indexes.SClear(key)
	case SetSExpire:
		if entry.Timestamp < uint64(time.Now().Unix()){
			db.setIndex.indexes.SClear(key)
		}else{
			db.expires[Set][key] = int64(entry.Timestamp)
		}
	}
}

func (db *StarDB) buildZsetIndex(idx *index.Indexer, entry *storage.Entry){
	if db.hashIndex == nil || idx == nil{
		return
	}

	key := string(idx.Meta.Key)
	switch entry.GetMark() {
	case ZSetZAdd:
		if score, err := utils.StrToFloat64(string(idx.Meta.Extra)); err == nil{
			db.zsetIndex.indexes.ZAdd(key, score, string(idx.Meta.Value))
		}
	case ZSetZRem:
		db.zsetIndex.indexes.ZRem(key, string(idx.Meta.Value))
	case ZSetZClear:
		db.zsetIndex.indexes.ZClear(key)
	case ZSetZExpire:
		if entry.Timestamp < uint64(time.Now().Unix()){
			db.zsetIndex.indexes.ZClear(key)
		} else {
			db.expires[ZSet][key] = int64(entry.Timestamp)
		}
	}
}