package index

import (
	"bytes"
	"math"
	"math/rand"
	"time"
)

//跳跃表

const (
	maxLevel	int = 18         //跳跃表最大层数
	probability float64 = 1 / math.E
)

type handleEle func(e *Element)bool

type (
	Node struct {
		next []*Element
	}

	Element struct {
		Node
		key []byte
		value interface{}
	}

	// SkipList 跳跃表定义
	SkipList struct {
		Node
		maxLevel	int          //最大层数
		Len         int
		randSource  rand.Source
		probability float64
		probTable   []float64
		prevNodesCache []*Node
	}
)

func NewSkipList() *SkipList {
	return &SkipList{
		Node: Node{next: make([]*Element, maxLevel)},
		prevNodesCache: make([]*Node, maxLevel),
		maxLevel: maxLevel,
		randSource: rand.New(rand.NewSource(time.Now().UnixNano())),
		probability: probability,
		probTable: probabilityTable(probability, maxLevel),
	}
}

func (e *Element) Key() []byte{
	return e.key
}

func (e *Element) Value() interface{}{
	return e.value
}

func (e *Element) SetValue(val interface{}){
	e.value = val
}

func (e *Element) Next() *Element {
	return e.next[0]
}

func (t *SkipList)Front()*Element{
	return t.next[0]
}

func (t *SkipList) Put(key []byte, value interface{}) *Element{
	var element *Element
	prev := t.backNodes(key)

	if element = prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		element.value = value
		return element
	}

	element = &Element{
		Node: Node{
			next: make([]*Element, t.randomLevel()),
		},
		key: key,
		value: value,
	}

	t.Len++
	return element
}

func (t *SkipList) Get(key []byte) *Element{
	var prev = &t.Node
	var next *Element

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next != nil && bytes.Compare(next.key, key) <= 0 {
		return next
	}

	return nil
}

func (t *SkipList) Remove(key []byte) *Element{
	prev := t.backNodes(key)

	if element := prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		for k, v := range element.next {
			prev[k].next[k] = v
		}

		t.Len--
		return element
	}
	return nil
}

func (t *SkipList) Foreach(fun handleEle){
	for p := t.Front(); p != nil; p = p.Next(){
		if ok := fun(p); !ok{
			break
		}
	}
}

func (t *SkipList) Exist(key []byte) bool {
	return t.Get(key) != nil
}

func (t *SkipList) backNodes(key []byte) []*Node{
	var prev = &t.Node
	var next *Element

	prevs := t.prevNodesCache

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.Node.next[i]
		}

		prevs[i] = prev
	}
	return prevs
}

func (t *SkipList) FindPrefix(prefix []byte) *Element{
	var prev = &t.Node
	var next *Element

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(prefix, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next == nil{
		next = t.Front()
	}

	return next
}

func (t *SkipList) randomLevel()(level int){
	r := float64(t.randSource.Int63())	/ (1 << 63)

	level = 1
	for level < t.maxLevel && r < t.probTable[level]{
		level++
	}
	return
}

func probabilityTable(probability float64, maxLevel int)(table []float64){
	for i := 1; i <= maxLevel; i++ {
		prob := math.Pow(probability, float64(i -1))
		table = append(table, prob)
	}
	return table
}