package hash

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

var key string = "my_hash"

func InitHash() *Hash{
	hash := New()
	hash.HSet(key, "math", []byte("hash_data_001"))
	hash.HSet(key, "english", []byte("hash_data_002"))
	hash.HSet(key, "chinese", []byte("hash_data_003"))
	return hash
}

func TestNew(t *testing.T) {
	hash := New()
	assert.NotEqual(t, hash, nil)
}

func TestHash_HSet(t *testing.T) {
	hash := InitHash()
	ret1 := hash.HSet(key, "math", []byte("123"))
	assert.Equal(t, ret1, 0)
	ret2 := hash.HSet(key, "zhongwen", []byte("汉字"))
	assert.Equal(t, ret2, 1)
}

func TestHash_HSetNx(t *testing.T) {
	hash := InitHash()
	ret1 := hash.HSetNx(key, "math", []byte("1223"))
	assert.Equal(t, ret1, 0)
	ret2 := hash.HSetNx(key, "zhongwen", []byte("zhongwen"))
	assert.Equal(t, ret2, 1)
}

func TestHash_HGet(t *testing.T) {
	hash := InitHash()
	ret1 := hash.HGet(key, "chinese")
	t.Log(string(ret1))
	ret2 := hash.HGet("aaa", "bbb")
	t.Log(string(ret2))
}

func TestHash_HGetAll(t *testing.T) {
	hash := InitHash()
	ret1 := hash.HGetAll(key)
	for _, v := range ret1{
		t.Log(string(v))
	}
}

func TestHash_HClear(t *testing.T) {
	hash := InitHash()
	ret1 := hash.HGetAll(key)
	for _, v := range ret1{
		t.Log(string(v))
	}
	hash.HClear(key)
	ret2 := hash.HGetAll(key)
	for _, v := range ret2{
		t.Log(string(v))
	}
}

func TestHash_HDel(t *testing.T) {
	hash := InitHash()
	ret1 := hash.HDel(key, "aaa")
	assert.Equal(t, ret1, 0)
	ret2 := hash.HDel(key, "math")
	assert.Equal(t, ret2, 1)
}

func TestHash_HExist(t *testing.T) {
	hash := InitHash()
	ret1 := hash.HExist(key, "yyy")
	assert.Equal(t, ret1, 0)
}

func TestHash_HLen(t *testing.T) {
	hash := InitHash()
	ret1 := hash.HLen(key)
	assert.Equal(t, ret1, 3)
	hash.HSet(key, "star", []byte("chen"))
	ret2 := hash.HLen(key)
	assert.Equal(t, ret2, 4)
}

func TestHash_HKeys(t *testing.T) {
	hash := InitHash()
	ret1 := hash.HKeys(key)
	fmt.Println(ret1)
}

func TestHash_HVals(t *testing.T) {
	hash := InitHash()
	ret1 := hash.HVals(key)
	for _, v := range ret1{
		t.Log(string(v))
	}
}

func TestHash_HKeyExists(t *testing.T) {
	hash := InitHash()
	ret1 := hash.HKeyExists(key)
	assert.Equal(t, ret1, true)
	ret2 := hash.HKeyExists("no")
	assert.Equal(t, ret2, false)
}