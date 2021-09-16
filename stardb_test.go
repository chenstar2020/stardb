package stardb

import(
	"encoding/json"
	"io/ioutil"
	"stardb/storage"
	"log"
	"testing"
)
var dbPath = "D:\\github\\stardb\\dbFile"

func InitDb() *StarDB{
	config := DefaultConfig()
	config.IdxMode = KeyOnlyMemMode
	config.RwMethod = storage.FileIO

	db, err := Open(config)
	if err != nil{
		log.Fatal(err)
	}

	return db
}

func TestOpen(t *testing.T) {
	opendb := func(method storage.FileRWMethod) {
		config := DefaultConfig()
		config.RwMethod = method

		config.DirPath = dbPath
		db, err := Open(config)
		if err != nil{
			t.Error("数据库打开失败：", err)
		}
		db.Close()
	}

	t.Run("FileIO", func(t *testing.T) {
		opendb(storage.FileIO)
	})

	t.Run("MMap", func(t *testing.T) {
		opendb(storage.MMap)
	})
}

func Test_SaveConfig(t *testing.T) {
	config := DefaultConfig()
	config.DirPath = dbPath

	db, err := Open(config)
	if err != nil{
		t.Error("数据库打开失败", err)
	}

	db.saveConfig()

	var cfg Config
	bytes, _ := ioutil.ReadFile(config.DirPath + "/db.cfg")
	_ = json.Unmarshal(bytes, &cfg)
	t.Logf("%+v", cfg)
}

func Test_SaveMeta(t *testing.T) {
	config := DefaultConfig()
	config.DirPath = dbPath

	db, err := Open(config)
	if err != nil{
		t.Error("数据库打开失败", err)
	}

	db.saveMeta()

	var cfg Config
	bytes, _ := ioutil.ReadFile(config.DirPath + "/db.META")
	_ = json.Unmarshal(bytes, &cfg)
	t.Logf("%+v", cfg)
}
