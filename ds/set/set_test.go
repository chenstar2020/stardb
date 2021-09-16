package set

import (
	"fmt"
	"testing"
)

var key = "my_set"
func NewSet()*Set{
	set := New()
	set.SAdd(key, []byte("aaa"))
	set.SAdd(key, []byte("bbb"))
	set.SAdd(key, []byte("ccc"))
	set.SAdd(key, []byte("ddd"))
	set.SAdd(key, []byte("eee"))
	set.SAdd(key, []byte("fff"))
	set.SAdd(key, []byte("ggg"))
	return set
}

func PrintSetData(set *Set, key string){
	members := set.SMembers(key)
	for _, v := range members{
		fmt.Println(string(v))
	}
}


func TestSet_SAdd(t *testing.T) {
	set := NewSet()
	set.SAdd(key, []byte("aaa"))
	ret := set.SAdd(key, []byte("bbb"))
	fmt.Println(ret)
}

func TestSet_SRem(t *testing.T) {
	set := NewSet()
	set.SAdd(key, []byte("aaa"))
	set.SAdd(key, []byte("bbb"))
	ret := set.SRem(key, []byte("bbb"))
	fmt.Println(ret)
}

func TestSet_SMove(t *testing.T) {
	var src = "src"
	var dst = "dst"
	set := NewSet()
	set.SAdd(src, []byte("aaa"))
	set.SAdd(src, []byte("bbb"))
	set.SAdd(dst, []byte("ccc"))
	ret := set.SMove(src, dst, []byte("aaa"))
	t.Log(ret)
	PrintSetData(set, src)
	fmt.Println("********************")
	PrintSetData(set, dst)
}

func TestSet_SPop(t *testing.T) {
	set := NewSet()
	members := set.SPop(key, 2)
	for _, v := range members{
		if set.SIsMember(key, v){
			t.Fatalf("set pop member:%v error", string(v))
		}
	}
}

func TestSet_SRandMember(t *testing.T) {
	set := NewSet()
	members := set.SRandMember(key, -20)
	for _, v := range members{
		fmt.Println(string(v))
	}
}

func TestSet_SCard(t *testing.T) {
	set := NewSet()
	ret := set.SCard(key)
	fmt.Println(ret)
	set.SRem(key, []byte("aaa"))
	ret2 := set.SCard(key)
	fmt.Println(ret2)
}

func TestSet_SMembers(t *testing.T) {
	set := NewSet()
	members := set.SMembers(key)
	for _, v := range members{
		fmt.Println(string(v))
	}
}

func TestSet_SUnion(t *testing.T) {
	set := NewSet()
	var key2 = "my_key2"
	set.SAdd(key2, []byte("zzz"))
	members := set.SUnion(key, key2)
	for _, v := range members{
		fmt.Println(string(v))
	}
}

func TestSet_SDiff(t *testing.T) {
	set := NewSet()
	var key2 = "my_key2"
	var key3 = "my_key3"
	set.SAdd(key2, []byte("aaa"))
	set.SAdd(key2, []byte("bbb"))
	set.SAdd(key3, []byte("ccc"))
	members := set.SDiff(key, key2, key3)
	for _, v := range members{
		fmt.Println(string(v))
	}
}