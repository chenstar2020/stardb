package zset

import (
	"math"
	"math/rand"
)

const (
	maxLevel	= 32
	probability = 0.25
)

type (
	SortedSet struct {
		record map[string]*SortedSetNode
	}

	SortedSetNode struct {
		dict map[string]*sklNode         //member到跳跃表结点的映射
		skl *skipList
	}

	sklLevel struct {
		forward *sklNode
		span    uint64
	}

	sklNode struct {
		member  string
		score   float64
		backward *sklNode
		level    []*sklLevel
	}

	skipList struct {
		head 	*sklNode  //头结点
		tail    *sklNode
		length  int64
		level   int16     //当前层
	}
)

func New() *SortedSet {
	return &SortedSet{
		make(map[string]*SortedSetNode),
	}
}

func (z *SortedSet)ZAdd(key string, score float64, member string){
	if !z.exist(key){
		node := &SortedSetNode{
			dict: make(map[string]*sklNode),
			skl: newSkipList(),                //每一个key维持一个跳跃表
		}
		z.record[key] = node
	}

	item := z.record[key]
	v, exist := item.dict[member]     //key和member都存在

	var node *sklNode
	if exist{
		if score != v.score {
			item.skl.sklDelete(v.score, member)
			node = item.skl.sklInsert(score, member)
		}
	}else{
		node = item.skl.sklInsert(score, member)
	}

	if node != nil{
		item.dict[member] = node
	}
}

func (z *SortedSet) ZRem(key, member string) bool{
	if !z.exist(key){
		return false
	}

	v, exist := z.record[key].dict[member]
	if exist {
		z.record[key].skl.sklDelete(v.score, member)
		delete(z.record[key].dict, member)
		return true
	}

	return false
}

func (z *SortedSet) ZKeyExists(key string) bool{
	return z.exist(key)
}

func (z *SortedSet) ZClear(key string){
	if z.ZKeyExists(key) {
		delete(z.record, key)
	}
}
//返回指定member的score值
func (z *SortedSet) ZScore(key string, member string) float64{
	if !z.exist(key){
		return math.MinInt64
	}

	node, exist := z.record[key].dict[member]
	if !exist{
		return math.MinInt64
	}
	return node.score
}

func (z *SortedSet) ZCard(key string) int{
	if !z.exist(key){
		return 0
	}

	return len(z.record[key].dict)
}

/*
 *返回成员的索引 成员按score从低到高排序
 */
func (z *SortedSet) ZRank(key, member string) int64{
	if !z.exist(key){
		return -1
	}

	v, exist := z.record[key].dict[member]
	if !exist{
		return -1
	}

	rank := z.record[key].skl.sklGetRank(v.score, member)
	rank--

	return rank
}
/*
 *返回成员的索引 成员按score从高到低排序
 */
func (z *SortedSet) ZRevRank(key, member string) int64{
	if !z.exist(key){
		return -1
	}

	v, exist := z.record[key].dict[member]
	if !exist{
		return -1
	}

	rank := z.record[key].skl.sklGetRank(v.score, member)

	return z.record[key].skl.length - rank
}
/*
 *给指定成员的score添加increment, 成员不存在则创建
 */
func (z *SortedSet) ZIncrBy(key string, increment float64, member string) float64{
	if z.exist(key){
		node, exist := z.record[key].dict[member]
		if exist{
			increment += node.score
		}
	}

	z.ZAdd(key, increment, member)
	return increment
}
/*
 *通过索引区间返回成员member
 */
func (z *SortedSet) ZRange(key string, start, stop int) []interface{}{
	if !z.exist(key){
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), false, false)
}
/*
 *通过索引区间返回成员member和score
 */
func (z *SortedSet) ZRangeWithScores(key string, start, stop int) []interface{}{
	if !z.exist(key){
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), false, true)
}
/*
 *通过索引区间返回成员member  成员按score从高到低排序
 */
func (z *SortedSet) ZRevRange(key string, start, stop int) []interface{}{
	if !z.exist(key){
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), true, false)
}
/*
 *通过索引区间返回成员member和score  成员按score从高到低排序
 */
func (z *SortedSet)ZRevRangeWithScores(key string, start, stop int)[]interface{}{
	if !z.exist(key){
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), true, true)
}
/*
 *通过索引返回成员member和score
 */
func (z *SortedSet) ZGetByRank(key string, rank int)(val []interface{}){
	if !z.exist(key){
		return
	}

	member, score := z.getByRank(key, int64(rank), false)
	val = append(val, member, score)
	return
}
/*
 *通过索引返回成员member和score 成员按score从高到低排序
 */
func (z *SortedSet) ZRevGetByRank(key string, rank int)(val []interface{}){
	if !z.exist(key){
		return
	}

	member, score := z.getByRank(key, int64(rank), true)
	val = append(val, member, score)
	return
}
/*
 *通过score返回成员member和score
 */
func (z *SortedSet) ZScoreRange(key string, min, max float64)(val []interface{}){
	if !z.exist(key) || min > max{
		return
	}

	item := z.record[key].skl
	minScore := item.head.level[0].forward.score
	if min < minScore{
		min = minScore
	}

	maxScore  := item.tail.score
	if max > maxScore{
		max = maxScore
	}

	p := item.head
	for i := item.level - 1; i >= 0; i--{
		for p.level[i].forward != nil && p.level[i].forward.score < min{
			p = p.level[i].forward
		}
	}

	p = p.level[0].forward
	for p != nil{
		if p.score > max{
			break
		}

		val = append(val, p.member, p.score)
		p = p.level[0].forward
	}
	return
}

/*
 *通过索引返回成员member和score 成员按score从高到低排序
 */
func (z *SortedSet) ZRevScoreRange(key string, max, min float64)(val []interface{}){
	if !z.exist(key) || max < min{
		return
	}

	item := z.record[key].skl
	minScore := item.head.level[0].forward.score
	if min < minScore{
		min = minScore
	}

	maxScore := item.tail.score
	if max > maxScore{
		max = maxScore
	}

	p := item.head
	for i := item.level - 1; i >= 0; i--{
		for p.level[i].forward != nil && p.level[i].forward.score <= max{
			p = p.level[i].forward
		}
	}

	for p != nil{
		if p.score < min{
			break
		}

		val = append(val, p.member, p.score)
		p = p.backward
	}

	return
}


func (z *SortedSet) exist(key string) bool{
	_, exist := z.record[key]
	return exist
}

func (z *SortedSet) getByRank(key string, rank int64, reverse bool)(string, float64){
	skl := z.record[key].skl
	if rank < 0 || rank > skl.length{
		return "", math.MinInt64
	}

	if reverse {
		 rank = skl.length - rank
	}else{
		rank++
	}

	n := skl.sklGetElementByRank(uint64(rank))
	if n == nil{
		return "", math.MinInt64
	}

	node := z.record[key].dict[n.member]
	if node == nil{
		return "", math.MinInt64
	}

	return node.member, node.score
}


func (z *SortedSet) findRange(key string, start, stop int64, reverse bool, withScores bool)(val []interface{}){
	skl := z.record[key].skl
	length := skl.length

	if start < 0{
		start += length
		if start < 0{
			start = 0
		}
	}

	if stop < 0{
		stop += length
	}

	if start > stop || start >= length{
		return
	}

	if stop >= length{
		stop = length - 1
	}
	span := (stop - start) + 1

	var node *sklNode
	if reverse{
		node = skl.tail
		if start > 0{
			node = skl.sklGetElementByRank(uint64(length - start))
		}
	} else {
		node = skl.head.level[0].forward
		if start > 0{
			node = skl.sklGetElementByRank(uint64(start + 1))
		}
	}

	for span > 0{
		span--
		if withScores{
			val = append(val, node.member, node.score)
		}else{
			val = append(val, node.member)
		}
		if reverse{
			node = node.backward
		}else{
			node = node.level[0].forward
		}
	}

	return
}

//跳跃表新结点
func sklNewNode(level int16, score float64, member string) *sklNode{
	node := &sklNode{
		score: score,
		member: member,
		level: make([]*sklLevel, level),
	}

	for i := range node.level{
		node.level[i] = new(sklLevel)
	}

	return node
}

func newSkipList() *skipList{
	return &skipList{
		level: 1,
		head: sklNewNode(maxLevel, 0, ""),
	}
}

func randomLevel() int16{
	var level int16 = 1
	for float32(rand.Int31()&0xFFFF) < (probability * 0xFFFF){
		level++
	}

	if level < maxLevel{
		return level
	}
	return maxLevel
}

func (skl *skipList) sklInsert(score float64, member string) *sklNode{
	updates := make([]*sklNode, maxLevel)  //需要更新的结点
	rank := make([]uint64, maxLevel)       //每一层的span汇总

	p := skl.head
	for i := skl.level - 1; i >= 0; i--{  //遍历每一层
		if i == skl.level - 1 {  //最高层
			rank[i] = 0
		} else {
			rank[i] = rank[i + 1]
		}

		if p.level[i] != nil {
			for p.level[i].forward != nil &&   //存在后继结点
				(p.level[i].forward.score < score ||   //后继结点小于当前结点
					(p.level[i].forward.score == score && p.level[i].forward.member < member)){

				rank[i] += p.level[i].span  //每一层的span累加
				p = p.level[i].forward
			}
		}
		updates[i] = p
	}

	level := randomLevel()
	if level > skl.level{
		for i := skl.level; i < level; i++ {
			rank[i] = 0
			updates[i] = skl.head
			updates[i].level[i].span = uint64(skl.length)
		}
		skl.level = level
	}

	p = sklNewNode(level, score, member)
	for i := int16(0); i < level; i++ {
		p.level[i].forward = updates[i].level[i].forward
		updates[i].level[i].forward = p

		p.level[i].span = updates[i].level[i].span - (rank[0] - rank[i])
		updates[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < skl.level; i++ {
		updates[i].level[i].span++
	}

	if updates[0] == skl.head{
		p.backward = nil
	}else{
		p.backward = updates[0]
	}

	if p.level[0].forward != nil {
		p.level[0].forward.backward = p
	}else{
		skl.tail = p
	}

	skl.length++
	return p
}

func (skl *skipList) sklDeleteNode(p *sklNode, updates []*sklNode){
	for i := int16(0); i < skl.level; i++{
		if updates[i].level[i].forward == p{
			updates[i].level[i].span += p.level[i].span - 1
			updates[i].level[i].forward = p.level[i].forward
		}else{
			updates[i].level[i].span--
		}
	}

	if p.level[0].forward != nil{
		p.level[0].forward.backward = p.backward
	}else{
		skl.tail = p.backward
	}

	for skl.level > 1 && skl.head.level[skl.level - 1].forward == nil{
		skl.level--
	}

	skl.length--
}

func (skl *skipList) sklDelete(score float64, member string){
	update := make([]*sklNode, maxLevel)
	p := skl.head

	for i := skl.level - 1; i >= 0; i--{
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member < member)) {
			p = p.level[i].forward
		}
		update[i] = p  //保存每一层需要更新的结点
	}


	p = p.level[0].forward  //经过上面的遍历之后 p指向被删除的结点
	if p != nil && score == p.score && p.member == member{
		skl.sklDeleteNode(p, update)
		return
	}
}

func (skl *skipList) sklGetRank(score float64, member string) int64{
	var rank uint64 = 0
	p := skl.head

	for i := skl.level - 1; i >= 0;i--{
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member <= member)){

			rank += p.level[i].span
			p = p.level[i].forward
		}

		if p.member == member{
			return int64(rank)
		}
	}
	return 0
}

func (skl *skipList) sklGetElementByRank(rank uint64)*sklNode{
	var traversed uint64 = 0
	p := skl.head

	for i := skl.level - 1; i >= 0; i--{
		for p.level[i].forward != nil && (traversed + p.level[i].span) <= rank{
			traversed += p.level[i].span
			p = p.level[i].forward
		}
		if traversed == rank{
			return p
		}
	}

	return nil
}