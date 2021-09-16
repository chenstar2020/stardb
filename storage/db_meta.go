package storage

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type DBMeta struct {
	ActiveWriteOff 		 map[uint16]int64 		 `json:"active_write_off"`     //当前活跃db的写偏移
	ReclaimableSpace     map[uint32]int64        `json:"reclaimable_space"`    //每个db文件的可回收空间
}

// LoadMeta 加载meta
func LoadMeta(path string)(m *DBMeta){
	m = &DBMeta{
		ActiveWriteOff: make(map[uint16]int64),
		ReclaimableSpace: make(map[uint32]int64),
	}

	file, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil{
		return
	}
	defer file.Close()

	b, _ := ioutil.ReadAll(file)
	_ = json.Unmarshal(b, m)
	return
}

func (m *DBMeta) Store(path string) error{
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil{
		return err
	}
	defer file.Close()

	b, _ := json.Marshal(m)
	_, err = file.Write(b)
	return err
}