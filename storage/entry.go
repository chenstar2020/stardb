package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"
)

var (
	ErrInvalidEntry = errors.New("storage/entry: invalid entry")

	ErrInvalidCrc   = errors.New("storage/entry: invalid crc")
)
const (
	//KeySize, ValueSize, ExtraSize, crc32 is uint32 type, 4 bytes each
	//Timestamp takes 8 bytes, state takes 2 bytes
	//4 * 4 + 8 + 2 = 26
	entryHeaderSize = 26
)

const (
	String uint16 = iota
	List
	Hash
	Set
	ZSet
)

type (
	Entry struct {
		Meta 		*Meta
		state       uint16
		crc32       uint32
		Timestamp   uint64
	}

	Meta struct {
		Key 		[]byte
		Value 		[]byte
		Extra   	[]byte
		KeySize 	uint32
		ValueSize 	uint32
		ExtraSize   uint32
	}
)

func newInternal(key, value, extra []byte, state uint16, timestamp uint64) *Entry {
	return &Entry{
		state: state,
		Timestamp: timestamp,
		Meta: &Meta{
			Key:	key,
			Value:  value,
			Extra:  extra,
			KeySize: 	uint32(len(key)),
			ValueSize: 	uint32(len(value)),
			ExtraSize: 	uint32(len(extra)),
		},
	}
}

func NewEntry(key, value, extra []byte, t, mark uint16)*Entry{
	var state uint16 = 0
	//set type and mark.
	state = state | (t << 8)
	state = state | mark
	return newInternal(key, value, extra, state, uint64(time.Now().Unix()))
}

func NewEntryNoExtra(key, value []byte, t, mark uint16)*Entry{
	return NewEntry(key, value, nil, t, mark)
}

func NewEntryWithExpire(key, value []byte, deadline int64, t, mark uint16) *Entry{
	var state uint16 = 0
	//set type and mark.
	state = state | (t << 8)
	state = state | mark

	return newInternal(key, value, nil, state, uint64(deadline))
}

func (e *Entry) Size() uint32 {
	return entryHeaderSize + e.Meta.KeySize + e.Meta.ValueSize + e.Meta.ExtraSize
}

func (e *Entry) Encode()([]byte, error){
	if e == nil || e.Meta.KeySize == 0{
		return nil, ErrInvalidEntry
	}

	ks, vs := e.Meta.KeySize, e.Meta.ValueSize
	es := e.Meta.ExtraSize
	buf := make([]byte, e.Size())

	binary.BigEndian.PutUint32(buf[4:8], ks)
	binary.BigEndian.PutUint32(buf[8:12], vs)
	binary.BigEndian.PutUint32(buf[12:16], es)
	binary.BigEndian.PutUint16(buf[16:18], e.state)
	binary.BigEndian.PutUint64(buf[18:26], e.Timestamp)
	copy(buf[entryHeaderSize:entryHeaderSize+ks], e.Meta.Key)
	copy(buf[entryHeaderSize+ks:(entryHeaderSize+ks+vs)], e.Meta.Value)
	if es > 0 {
		copy(buf[(entryHeaderSize+ks+vs):(entryHeaderSize+ks+vs+es)], e.Meta.Extra)
	}

	crc := crc32.ChecksumIEEE(e.Meta.Value)
	binary.BigEndian.PutUint32(buf[0:4], crc)  //0:4 crc of value

	return buf, nil
}

func Decode(buf []byte)(*Entry, error){
	ks := binary.BigEndian.Uint32(buf[4:8])
	vs := binary.BigEndian.Uint32(buf[8:12])
	es := binary.BigEndian.Uint32(buf[12:16])
	state := binary.BigEndian.Uint16(buf[16:18])
	timestamp := binary.BigEndian.Uint64(buf[18:26])
	crc := binary.BigEndian.Uint32(buf[0:4])

	return &Entry{
		Meta :&Meta{
			KeySize: ks,
			ValueSize: vs,
			ExtraSize: es,
		},
		state: state,
		crc32: crc,
		Timestamp: timestamp,
	}, nil
}

func (e *Entry) GetType() uint16{
	return e.state >> 8
}

func (e *Entry) GetMark() uint16{
	return e.state & (2<<7 - 1)
}