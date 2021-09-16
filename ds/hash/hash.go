package hash


type (
	Hash struct {
		record Record
	}
	// Record 存储hash记录
	Record map[string]map[string][]byte
)

type (
	Fields struct{
		filed string
		value []byte
	}
)

func New()*Hash{
	return &Hash{make(Record)}
}

/*
 如果key field存在 value会被覆盖掉
 @return 1: key field存在 0:key field 不存在
 */
func (h *Hash) HSet(key string, field string, value []byte)(res int){
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}

	if h.record[key][field] != nil{
		h.record[key][field] = value
	}else{
		h.record[key][field] = value
		res = 1
	}
	return
}

/*
 当key field不存在 value才会写入
 @return 1: key field存在 0:key field 不存在
*/
func (h *Hash) HSetNx(key string, field string, value []byte) int{
	if !h.exist(key){
		h.record[key] = make(map[string][]byte)
	}

	if _, exist := h.record[key][field]; !exist{
		h.record[key][field] = value
		return 1
	}
	return 0
}

func (h *Hash) HGet(key, field string) []byte{
	if !h.exist(key){
		return nil
	}

	return h.record[key][field]
}

func (h *Hash) HGetAll(key string)(res [][]byte){
	if !h.exist(key){
		return
	}

	for k, v := range h.record[key] {
		res = append(res, []byte(k), v)
	}
	return
}

func (h *Hash) HClear(key string){
	if !h.exist(key){
		return
	}

	delete(h.record, key)
}

func (h *Hash) HDel(key, field string) int{
	if !h.exist(key){
		return 0
	}

	if _, exist := h.record[key][field]; exist{
		delete(h.record[key], field)
		return 1
	}
	return 0
}

func (h *Hash) HKeyExists(key string) bool{
	return h.exist(key)
}

func (h *Hash) HExist(key, field string) (res int){
	if !h.exist(key){
		return
	}

	if _, exist := h.record[key][field]; exist{
		res = 1
	}
	return
}

func (h *Hash) HLen(key string)(res int){
	if !h.exist(key){
		return
	}
	return len(h.record[key])
}

func (h *Hash) HKeys(key string)(val []string){
	if !h.exist(key){
		return
	}

	for k := range h.record[key]{
		val = append(val, k)
	}
	return
}

func (h *Hash) HVals(key string)(val [][]byte){
	if !h.exist(key){
		return
	}

	for _, v := range h.record[key]{
		val = append(val, v)
	}
	return
}


func (h *Hash) HMSet(key string,  fields ...Fields)(res int) {
	for i := 0; i < len(fields); i++ {
		res += h.HSet(key, fields[i].filed, fields[i].value)
	}
	return
}


func (h *Hash) HMGet(key string, fields ...string)(res [][]byte){
	for i := 0; i < len(fields); i++ {
		res = append(res, h.HGet(key, fields[i]))
	}
	return
}
func (h *Hash) exist(key string) bool{
	_, exist := h.record[key]
	return exist
}