package index

import (
	"fmt"
	"testing"
)


func TestNewSkipList(t *testing.T) {
	sklList := NewSkipList()
	if sklList == nil{
		t.Error("new skl err")
	}
}

func TestSkipList_Exist(t *testing.T) {
	sklList := NewSkipList()
	exist := sklList.Exist([]byte("aaa"))
	t.Log(exist)
}

func TestSkipList_FindPrefix(t *testing.T) {
	list := NewSkipList()
	val := []byte("test_val")

	list.Put([]byte("ec"), val)
	list.Put([]byte("dc"), val)
	list.Put([]byte("ac"), val)
	list.Put([]byte("ae"), val)
	list.Put([]byte("bc"), val)
	list.Put([]byte("22"), val)

	ele := list.FindPrefix([]byte("a"))
	if ele != nil{
		fmt.Println("aaaaa", string(ele.key))
	}

}

func TestSkipList_Put(t *testing.T) {
	list := NewSkipList()
	val := []byte("test_val")

	ele := list.Put([]byte("ec"), val)
	if ele != nil{
		t.Log("list.Put Ok")
		fmt.Println(string(ele.key))
	}
}

func TestSkipList_Get(t *testing.T) {
	list := NewSkipList()
	val := []byte("test_val")

	list.Put([]byte("ec"), val)
	list.Put([]byte("dc"), val)

	data := list.Get([]byte("ec"))
	if data != nil{
		fmt.Println(string(data.key))
	}else{
		t.Log("list.Get error")
	}

}