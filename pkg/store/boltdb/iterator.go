package boltdb

import (
	"github.com/boltdb/bolt"
)

type Iterator struct {
	tx    *bolt.Tx
	it    *bolt.Cursor
	key   []byte
	value []byte
}

func (it *Iterator) Close() {
	if it.tx != nil {
		it.tx.Rollback()
	}
}

func (it *Iterator) SeekTo(key []byte) []byte {
	it.key, it.value = it.it.Seek(key)
	return key
}

func (it *Iterator) SeekToFirst() {
	it.it.First()
}

func (it *Iterator) Next() {
	it.key, it.value = it.it.Next()
}

func (it *Iterator) Valid() bool {
	return !(it.key == nil && it.value == nil)
}

func (it *Iterator) Key() []byte {
	return it.key
}
func (it *Iterator) Value() []byte {
	return it.value
}

func (it *Iterator) Error() error {
	return nil
}
