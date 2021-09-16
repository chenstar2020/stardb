package zset

import (
	"fmt"
	"testing"
)

var key = "myZSet"
func InitZSet() *SortedSet{
	zSet := New()
	zSet.ZAdd(key, 20, "bbb")
	zSet.ZAdd(key, 22, "ddd")
	zSet.ZAdd(key, 19, "aaa")
	zSet.ZAdd(key, 23, "eee")
	zSet.ZAdd(key, 24, "fff")
	zSet.ZAdd(key, 24, "ffe")
	zSet.ZAdd(key, 21, "ccc")
	return zSet
}

func TestSortedSet_ZAdd(t *testing.T) {
	t.Run("normal data", func(t *testing.T) {
		zSet := InitZSet()
		zSet.ZAdd(key, 25, "hahahha")
		fmt.Println(zSet.ZCard(key))
	})
}

func TestSortedSet_ZRem(t *testing.T) {
	zSet := InitZSet()
	zSet.ZRem(key, "ccc")
	t.Log(zSet.ZScore(key, "ccc"))
}

func TestSortedSet_ZScore(t *testing.T) {
	zSet := InitZSet()
	t.Log(zSet.ZScore(key, "aaa"))
}

func TestSortedSet_ZRank(t *testing.T) {
	zSet := InitZSet()
	fmt.Println(zSet.ZRank(key, "ffe"))
}

func TestSortedSet_ZRevRank(t *testing.T) {
	zSet := InitZSet()
	fmt.Println(zSet.ZRevRank(key, "ffe"))
}

func TestSortedSet_ZIncrBy(t *testing.T) {
	zSet := InitZSet()
	zSet.ZIncrBy(key, 10, "aaa")
	t.Log(zSet.ZScore(key, "aaa"))
}

func TestSortedSet_ZRange(t *testing.T) {
	zSet := InitZSet()
	data := zSet.ZRange(key, 1, 3)
	for _, v := range data{
		fmt.Println(v)
	}
}

func TestSortedSet_ZRangeWithScores(t *testing.T) {
	zSet := InitZSet()
	data := zSet.ZRangeWithScores(key, 5, 23)
	fmt.Println("len:", len(data))
	for _, v := range data{
		fmt.Printf("%+v\n", v)
	}
}

func TestSortedSet_ZRevRangeWithScores(t *testing.T) {
	zSet := InitZSet()
	data := zSet.ZRevRangeWithScores(key, 0, -1)
	fmt.Println("len:", len(data))
	for _, v := range data {
		fmt.Printf("%+v\n", v)
	}
}

func TestSortedSet_ZGetByRank(t *testing.T) {
	zSet := InitZSet()
	zSet.ZAdd(key, 24, "fff")
	zSet.ZAdd(key, 24, "fff")
	data := zSet.ZGetByRank(key, 5)
	for _, v := range data{
		fmt.Printf("%+v\n", v)
	}
}

func TestSortedSet_ZRevGetByRank(t *testing.T) {
	zSet := InitZSet()
	data := zSet.ZRevGetByRank(key, 3)
	for _, v := range data{
		fmt.Printf("%+v\n", v)
	}
}

func TestSortedSet_ZScoreRange(t *testing.T) {
	zSet := InitZSet()
	data := zSet.ZScoreRange(key, 19, 23)
	for _, v := range data{
		fmt.Printf("%+v\n", v)
	}
}

func TestSortedSet_ZRevScoreRange(t *testing.T) {
	zSet := InitZSet()
	data := zSet.ZRevScoreRange(key, 23, 19)
	for _, v := range data{
		fmt.Printf("%+v\n", v)
	}
}