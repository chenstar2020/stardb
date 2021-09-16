package cmd

import (
	"github.com/tidwall/redcon"
	"stardb"
	"strconv"
)
func set(db *stardb.StarDB, args []string) (res interface{}, err error) {
	if len(args) != 2{
		err = newWrongNumOfArgsError("set")
		return
	}

	key, value := args[0], args[1]
	if err = db.Set([]byte(key), []byte(value)); err == nil{
		res = okResult
	}
	return
}

func get(db *stardb.StarDB, args []string) (res interface{}, err error) {
	if len(args) != 1{
		err = newWrongNumOfArgsError("get")
		return
	}
	key := args[0]
	var val []byte
	if val, err = db.Get([]byte(key)); err == nil{
		res = string(val)
	}
	return
}

func setNx(db *stardb.StarDB, args []string) (res interface{}, err error) {
	if len(args) != 2{
		err = newWrongNumOfArgsError("setnx")
		return
	}

	key, value := args[0], args[1]
	result, err := db.SetNx([]byte(key), []byte(value))

	if err == nil{
		res = result
	}
	return
}

func getSet(db *stardb.StarDB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("getset")
		return
	}
	key, value := args[0], args[1]
	var val []byte
	if val, err = db.GetSet([]byte(key), []byte(value)); err == nil{
		res = string(val)
	}
	return
}

func appendStr(db *stardb.StarDB, args []string) (res interface{}, err error) {
	if len(args) != 2{
		err = newWrongNumOfArgsError("append")
		return
	}
	key, value := args[0], args[1]
	if err = db.Append([]byte(key), []byte(value)); err == nil{
		res = okResult
	}
	return
}

func strLen(db *stardb.StarDB, args []string) (res interface{}, err error) {
	if len(args) != 1{
		err = newWrongNumOfArgsError("strlen")
		return
	}
	length := db.StrLen([]byte(args[0]))
	res = redcon.SimpleInt(length)
	return
}

func strExists(db *stardb.StarDB, args []string) (res interface{}, err error) {
	if len(args) != 1{
		err = newWrongNumOfArgsError("strexists")
		return
	}
	if exists := db.StrExists([]byte(args[0])); exists{
		res = redcon.SimpleInt(1)
	}else{
		res = redcon.SimpleInt(0)
	}
	return
}

func strRem(db *stardb.StarDB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("strrem")
		return
	}
	if err = db.StrRem([]byte(args[0])); err == nil{
		res = okResult
	}
	return
}

func prefixScan(db *stardb.StarDB, args []string) (res interface{}, err error) {
	if len(args) != 3{
		err = newWrongNumOfArgsError("prefixscan")
		return
	}
	limit, err := strconv.Atoi(args[1])
	if err != nil{
		err = ErrSyntaxIncorrect
		return
	}
	offset, err := strconv.Atoi(args[2])
	if err != nil{
		err = ErrSyntaxIncorrect
		return
	}

	res, err = db.PrefixScan(args[0], limit, offset)
	return
}

func rangeScan(db *stardb.StarDB, args []string) (res interface{}, err error) {
	if len(args) != 2{
		err = newWrongNumOfArgsError("rangescan")
		return
	}

	res, err = db.RangeScan([]byte(args[0]), []byte(args[1]))
	return
}

func expire(db *stardb.StarDB, args []string) (res interface{}, err error) {
	if len(args) != 2{
		err = ErrSyntaxIncorrect
		return
	}
	seconds, err := strconv.Atoi(args[1])
	if err != nil{
		err = ErrSyntaxIncorrect
		return
	}
	if err = db.Expire([]byte(args[0]), int64(seconds)); err == nil{
		res = okResult
	}
	return
}

func persist(db *stardb.StarDB, args []string) (res interface{}, err error) {
	if len(args) != 1{
		err = newWrongNumOfArgsError("persist")
		return
	}
	db.Persist([]byte(args[0]))
	res = okResult
	return
}

func ttl(db *stardb.StarDB, args []string) (res interface{}, err error) {
	if len(args) != 1{
		err = newWrongNumOfArgsError("ttl")
		return
	}
	ttlVal := db.TTL([]byte(args[0]))
	res = strconv.FormatInt(int64(ttlVal), 0)
	return
}

func init(){
	addExecCommand("set", set)
	addExecCommand("get", get)
	addExecCommand("setnx", setNx)
	addExecCommand("getset", getSet)
	addExecCommand("append", appendStr)
	addExecCommand("strlen", strLen)
	addExecCommand("strexists", strExists)
	addExecCommand("strrem", strRem)
	addExecCommand("prefixscan", prefixScan)
	addExecCommand("rangescan", rangeScan)
	addExecCommand("expire", expire)
	addExecCommand("persist", persist)
	addExecCommand("ttl", ttl)
}
