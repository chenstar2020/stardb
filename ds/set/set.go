package set

var existFlag = struct {}{}

type (
	Set struct {
		record Record
	}

	// Record 存储set记录
	Record map[string]map[string]struct{}
)

func New() *Set {
	return &Set{
		make(Record),
	}
}

func (s *Set) SAdd(key string, member []byte) int{
	if !s.exist(key){
		s.record[key] = make(map[string]struct{})
	}

	s.record[key][string(member)] = existFlag
	return len(s.record[key])
}

func (s *Set) SRem(key string, member []byte) bool{
	if !s.exist(key){
		return false
	}

	if _, ok := s.record[key][string(member)];ok{
		delete(s.record[key], string(member))
		return true
	}
	return false
}

/*
 *move member from src to dst
 */
func (s *Set) SMove(src, dst string, member []byte) bool{
	if !s.fieldExist(src, string(member)){
		return false
	}

	if !s.exist(dst){
		s.record[dst] = make(map[string]struct{})
	}

	delete(s.record[src], string(member))
	s.record[dst][string(member)] = existFlag
	return true
}

func (s *Set) SPop(key string, count int)[][]byte{
	var val [][]byte
	if !s.exist(key) || count <= 0{
		return val
	}

	for k := range s.record[key] {
		delete(s.record[key], k)
		val = append(val, []byte(k))

		count--
		if count == 0{
			break
		}
	}
	return val
}

func (s *Set) SClear(key string){
	if s.SKeyExists(key){
		delete(s.record, key)
	}
}

func (s *Set) SKeyExists(key string)bool{
	return s.exist(key)
}

func (s *Set) SIsMember(key string, member []byte)bool{
	return s.fieldExist(key, string(member))
}

func (s *Set) SRandMember(key string, count int)[][]byte{
	var val [][]byte
	if ! s.exist(key) || count == 0{
		return val
	}

	if count > 0{
		for k := range s.record[key]{
			val = append(val, []byte(k))
			if len(val) == count{
				break
			}
		}
	}else{
		count = -count
		randomVal := func() []byte{
			for k := range s.record[key]{
				return []byte(k)
			}
			return nil
		}
		for count > 0{
			val = append(val, randomVal()) //可能有重复的值
			count--
		}
	}
	return val
}

func (s *Set) SCard(key string) int{
	if !s.exist(key){
		return 0
	}

	return len(s.record[key])
}

func (s *Set) SMembers(key string) (val [][]byte){
	if !s.exist(key){
		return
	}

	for k := range s.record[key]{
		val = append(val, []byte(k))
	}
	return
}

func (s *Set) SUnion(keys ...string)(val [][]byte){
	for _, k := range keys{
		if s.exist(k){
			for v := range s.record[k]{
				val = append(val, []byte(v))
			}
		}
	}
	return
}

func (s *Set)SDiff(keys ...string)(val [][]byte){
	if len(keys) == 0 || !s.exist(keys[0]){
		return
	}

	for v := range s.record[keys[0]]{
		flag := true
		for i := 1; i < len(keys); i++{
			if s.SIsMember(keys[i], []byte(v)){
				flag = false
				break
			}
		}
		if flag{
			val = append(val, []byte(v))
		}
	}
	return
}


func (s *Set) exist(key string) bool {
	_, exist := s.record[key]
	return exist
}

func (s *Set) fieldExist(key, filed string) bool{
	fields, exist := s.record[key]
	if !exist{
		return false
	}

	_, ok := fields[filed]
	return ok
}