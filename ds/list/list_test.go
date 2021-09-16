package list

import (
	"fmt"
	"github.com/magiconair/properties/assert"
	"testing"
)

var key = "my_list"

func InitList() *List {
	list := New()

	list.LPush(key, []byte("a"), []byte("b"), []byte("c"))
	list.LPush(key, []byte("d"), []byte("e"), []byte("f"))

	return list
}

func PrintListData(lis *List){
	if lis.record[key] == nil || lis.record[key].Len() <= 0{
		fmt.Println("list is empty")
		return
	}

	for p := lis.record[key].Front(); p != nil; p = p.Next(){
		fmt.Print(string(p.Value.([]byte)), " ")
	}
	fmt.Println()
}

func PrintListKeyData(lis *List, key string){
	if lis.record[key] == nil || lis.record[key].Len() <= 0{
		fmt.Println("list is empty")
		return
	}

	for p := lis.record[key].Front(); p != nil; p = p.Next(){
		fmt.Print(string(p.Value.([]byte)), " ")
	}
	fmt.Println()
}

func TestList_LPush(t *testing.T) {
	list := InitList()

	size := list.LPush(key, []byte("stardb"))
	size = list.LPush(key, []byte("stardb2"))
	PrintListData(list)
	fmt.Println("size = ", size)
}

func TestList_LPop(t *testing.T) {
	list := InitList()

	list.LPush(key, []byte("aaa"), []byte("bbb"))

	PrintListData(list)
	val := list.LPop(key)
	fmt.Println(string(val))
	PrintListData(list)

}

func TestList_RPush(t *testing.T) {
	list := InitList()

	size := list.RPush(key, []byte("stardb"))
	PrintListData(list)
	t.Logf("size = %v", size)
}

func TestList_RPop(t *testing.T) {
	list := InitList()

	PrintListData(list)
	val := list.RPop(key)
	fmt.Println(string(val))
	PrintListData(list)
}

func TestList_LIndex(t *testing.T) {
	list := InitList()
	PrintListData(list)
	val := list.LIndex(key, -2)
	fmt.Println(string(val))
}

func TestList_LRem(t *testing.T) {
	list := InitList()

	list.LPush(key, []byte("a"), []byte("t"), []byte("c"), []byte("a"))
	PrintListData(list)
	rem := list.LRem(key, []byte("a"), 1)
	PrintListData(list)
	fmt.Println(rem)
	ok := list.LValExists(key, []byte("vvv"))
	fmt.Println(ok)
}

func TestList_LInsert(t *testing.T) {
	list := InitList()
    //子测试 before
	t.Run("before", func(t *testing.T) {
		n := list.LInsert(key, Before, []byte("a"), []byte("AA"))
		t.Log(n)

		PrintListData(list)
	})

    //子测试 after
	t.Run("after", func(t *testing.T) {
		n := list.LInsert(key, After, []byte("e"), []byte("EE"))
		t.Log(n)

		PrintListData(list)
	})
}

func TestList_LSet(t *testing.T) {
	list := InitList()
	PrintListData(list)
	list.LSet(key, 0, []byte("dd"))
	PrintListData(list)
	fmt.Println(list.LValExists(key, []byte("dd")))
}

func TestList_LRange(t *testing.T) {
	list := InitList()
	PrintListData(list)
	ret := list.LRange(key, -3, -1)
	for _, v := range ret{
		fmt.Println(string(v))
	}
}

func TestList_LTrim(t *testing.T) {
	list := InitList()
	PrintListData(list)
	list.LTrim(key, 12 ,9)
	PrintListData(list)
}

func TestList_LLen(t *testing.T) {
	list := InitList()
	PrintListData(list)
	fmt.Println(list.LLen(key))
}

func TestList_LClear(t *testing.T) {
	list := InitList()
	PrintListData(list)
	list.LClear(key)
	PrintListData(list)
}

func TestList_LKeyExists(t *testing.T) {
	list := InitList()
	ret1 := list.LKeyExists(key)
	assert.Equal(t, ret1, true)
	ret2 := list.LKeyExists("no key")
	assert.Equal(t, ret2, false)
}

func TestList_LValExists(t *testing.T) {
	list := InitList()

	ok1 := list.LValExists(key, []byte("a"))
	t.Log(ok1)

	ok2 := list.LValExists(key, []byte("bbb"))
	t.Log(ok2)
}