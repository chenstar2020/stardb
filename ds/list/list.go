package list

import (
	"container/list"
	"reflect"
)

// InsertOption 插入方式
type InsertOption uint8

const (
	// Before 前插
	Before InsertOption	= iota
	// After 尾插
	After
)

type (
	List struct {
		record Record

		values map[string]map[string]int  //在LValExists函数中起作用
	}

	Record map[string]*list.List
)

func New() *List {
	return &List{
		make(Record),
		make(map[string]map[string]int),
	}
}

func (lis *List) LPush(key string, val ...[]byte) int {
	return lis.push(true, key, val...)
}

func (lis *List) LPop(key string) []byte{
	return lis.pop(true, key)
}

func (lis *List) RPush(key string, val ...[]byte) int {
	return lis.push(false, key, val...)
}

func (lis *List) RPop(key string) []byte{
	return lis.pop(false, key)
}

func (lis *List) LIndex(key string, index int) []byte{
/*	ok, newIndex := lis.validIndex(key, index)
	if !ok {
		return nil
	}

	index = newIndex*/

	var val []byte
	e := lis.index(key, index)
	if e != nil{
		val = e.Value.([]byte)
	}

	return val
}

/*
 *移除list中count个数量的val
 *count = 0 移除list中所有的val
 *count > 0 从前向后移除list中count个val
 *count < 0 从后向前移除list中count个val
 */
func (lis *List) LRem(key string, val []byte, count int) int{
	item := lis.record[key]
	if item == nil{
		return 0
	}

	var ele []*list.Element
	if count == 0{
		for p := item.Front(); p != nil; p = p.Next(){
			if reflect.DeepEqual(p.Value.([]byte), val){
				ele = append(ele, p)
			}
		}
	}
	if count > 0{
		for p := item.Front(); p != nil && len(ele) < count; p = p.Next(){
			if reflect.DeepEqual(p.Value.([]byte), val){
				ele = append(ele, p)
			}
		}
	}
	if count < 0{
		for p := item.Back(); p != nil && len(ele) < -count; p = p.Prev(){
			if reflect.DeepEqual(p.Value.([]byte), val){
				ele = append(ele, p)
			}
		}
	}

	for _, e := range ele {
		item.Remove(e)
	}
	length := len(ele)
	ele = nil

	if lis.values[key] != nil{
		cnt := lis.values[key][string(val)] - length
		if cnt <= 0{
			delete(lis.values[key], string(val))
		}else{
			lis.values[key][string(val)] = cnt
		}
	}
	return length
}
/*
 *在pivot之前(后）插入val
 */
func (lis *List) LInsert(key string, option InsertOption, pivot, val []byte)int{
	e := lis.find(key, pivot)
	if e == nil{
		return -1
	}

	item := lis.record[key]
	if option == Before{
		item.InsertBefore(val, e)
	}
	if option == After{
		item.InsertAfter(val, e)
	}

	if lis.values[key] == nil{
		lis.values[key] = make(map[string]int)
	}
	lis.values[key][string(val)] += 1

	return item.Len()
}

func (lis *List) LSet(key string, index int, val []byte) bool{
	e := lis.index(key, index)
	if e == nil{
		return false
	}

	if lis.values[key] == nil{
		lis.values[key] = make(map[string]int)
	}

	if e.Value != nil{
		v := string(e.Value.([]byte))
		cnt := lis.values[key][v] - 1
		if cnt <= 0{
			delete(lis.values[key], v)
		}else{
			lis.values[key][v] = cnt
		}
	}

	e.Value = val
	lis.values[key][string(val)] += 1
	return true
}

func (lis *List) LRange(key string, start, end int) [][]byte{
	var val [][]byte
	item := lis.record[key]

	if item == nil || item.Len() <= 0{
		return val
	}

	length := item.Len()
	start, end = lis.handleIndex(length, start, end)

	if start > end || start >= length{
		return val
	}

	mid := length >> 1

	if end <= mid || end - mid < mid - start{  //star和end的中位数是否小于mid
		flag := 0
		for p := item.Front(); p != nil && flag <= end; p, flag = p.Next(), flag+1 {
			if flag >= start{
				val = append(val, p.Value.([]byte))
			}
		}
	}else{
		flag := length - 1
		for p := item.Back(); p != nil && flag >= start; p, flag = p.Prev(), flag-1{
			if flag <= end{
				val = append(val, p.Value.([]byte))
			}
		}
		//倒序
		if len(val) > 0{
			for i, j := 0, len(val)-1; i < j; i, j = i+1,j-1{
				val[i], val[j] = val[j], val[i]
			}
		}
	}
	return val
}

/*
 * 裁剪链表 只保留start ~ end范围内的元素
 */
func (lis *List) LTrim(key string, start, end int) bool{
	item := lis.record[key]
	if item == nil || item.Len() <= 0 {
		return false
	}

	length := item.Len()
	start, end = lis.handleIndex(length, start, end)
	if start <= 0 && end >= length - 1{
		return false
	}

	if start > end || start >= length {
		lis.record[key] = nil
		lis.values[key] = nil
		return true
	}

	startEle, endEle := lis.index(key, start), lis.index(key, end)
	if end - start + 1 < (length >> 1){  //小于原长度的一半
		newList := list.New()
		newValuesMap := make(map[string]int)
		for p := startEle; p != endEle.Next();p = p.Next(){
			newList.PushBack(p.Value)
			if p.Value != nil{
				newValuesMap[string(p.Value.([]byte))] += 1
			}
		}

		item = nil  //释放掉之前的内存
		lis.record[key] = newList
		lis.values[key] = newValuesMap
	} else {
		var ele []*list.Element
		for p := item.Front(); p != startEle; p = p.Next(){
			ele = append(ele, p)
		}
		for p := item.Back(); p != endEle; p = p.Prev(){
			ele = append(ele, p)
		}

		for _, e := range ele{
			item.Remove(e)
			if lis.values[key] != nil && e.Value != nil {
				v := string(e.Value.([]byte))
				cnt := lis.values[key][v] - 1
				if cnt <= 0{
					delete(lis.values[key], v)
				}else{
					lis.values[key][v] = cnt
				}
			}
		}
		ele = nil
	}
	return true
}

func (lis *List) LLen(key string)int{
	length := 0
	if lis.record[key] != nil{
		length = lis.record[key].Len()
	}
	return length
}

func (lis *List) LClear(key string){
	delete(lis.record, key)
	delete(lis.values, key)
}

func (lis *List) LKeyExists(key string)(ok bool){
	_, ok = lis.record[key]
	return ok
}

func (lis *List) LValExists(key string, val []byte)(ok bool){
	if lis.values[key] != nil{
		cnt := lis.values[key][string(val)]
		ok = cnt > 0
	}
	return
}

func (lis *List) find(key string, val []byte) *list.Element{
	item := lis.record[key]
	var e *list.Element

	if item != nil{
		for p := item.Front(); p != nil; p = p.Next(){
			if reflect.DeepEqual(p.Value.([]byte), val){
				e = p
				break
			}
		}
	}
	return e
}

func (lis *List) index(key string, index int) *list.Element{
	ok, newIndex := lis.validIndex(key, index)
	if !ok{
		return nil
	}

	index = newIndex
	item := lis.record[key]
	var e *list.Element

	if item != nil && item.Len() > 0{
		if index <= (item.Len() >> 1){
			val := item.Front()
			for i := 0; i < index; i++ {
				val = val.Next()
			}
			e = val
		}else{
			val := item.Back()
			for i := item.Len() - 1; i > index; i--{
				val = val.Prev()
			}
			e = val
		}
	}

	return e
}

func (lis *List) validIndex(key string, index int)(bool, int){
	item := lis.record[key]
	if item == nil || item.Len() <= 0{
		return false, index
	}

	length := item.Len()
	if index < 0 {          //index小于0  从后往前数
		index += length
	}

	return index >= 0 && index < length, index
}

func (lis *List) push(front bool, key string, val ...[]byte) int {
	if lis.record[key] == nil{
		lis.record[key] = list.New()
	}
	if lis.values[key] == nil{
		lis.values[key] = make(map[string]int)
	}

	for _, v := range val{
		if front{
			lis.record[key].PushFront(v)
		}else{
			lis.record[key].PushBack(v)
		}
		lis.values[key][string(v)] += 1
	}
	return lis.record[key].Len()
}

func (lis *List) pop(front bool, key string)[]byte{
	item := lis.record[key]
	var val []byte

	if item != nil && item.Len() > 0 {
		var e *list.Element
		if front {
			e = item.Front()
		} else {
			e = item.Back()
		}

		val = e.Value.([]byte)
		item.Remove(e)

		if lis.values[key] != nil{
			cnt := lis.values[key][string(val)] - 1
			if cnt <= 0 {
				delete(lis.values[key], string(val))
			}else{
				lis.values[key][string(val)] = cnt
			}
		}
	}
	return val
}

/*
 *return start:  0 ~ +∞
 *return end:   -∞ ~ length - 1
 */
func (lis *List) handleIndex(length, start, end int)(int, int){
	if start < 0{
		start += length
	}

	if end < 0{
		end += length
	}

	if start < 0 {
		start = 0
	}

	if end >= length{
		end = length - 1
	}

	return start, end
}