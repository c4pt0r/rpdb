// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package rpdb

import (
	"bytes"

	"github.com/wandoulabs/rpdb/pkg/store"
	"github.com/wandoulabs/redis-port/pkg/rdb"
)

type rpdbIterator struct {
	store.Iterator
	serial uint64
}

type rpdbReader interface {
	getRowValue(key []byte) ([]byte, error)
	getIterator() *rpdbIterator
	putIterator(it *rpdbIterator)
}

func loadObjEntry(r rpdbReader, db uint32, key []byte) (rpdbRow, *rdb.ObjEntry, error) {
	o, err := loadRpdbRow(r, db, key)
	if err != nil || o == nil {
		return o, nil, err
	}
	if o.IsExpired() {
		return o, nil, nil
	}
	if val, err := o.loadObjectValue(r); err != nil {
		return o, nil, err
	} else {
		obj := &rdb.ObjEntry{
			DB:       db,
			Key:      key,
			Value:    val,
			ExpireAt: o.GetExpireAt(),
		}
		return o, obj, nil
	}
}

func loadBinEntry(r rpdbReader, db uint32, key []byte) (rpdbRow, *rdb.BinEntry, error) {
	o, obj, err := loadObjEntry(r, db, key)
	if err != nil || obj == nil {
		return o, nil, err
	}
	if bin, err := obj.BinEntry(); err != nil {
		return o, nil, err
	} else {
		return o, bin, nil
	}
}

func firstKeyUnderSlot(r rpdbReader, db uint32, slot uint32) ([]byte, error) {
	it := r.getIterator()
	defer r.putIterator(it)
	pfx := EncodeMetaKeyPrefixSlot(db, slot)
	if it.SeekTo(pfx); it.Valid() {
		metaKey := it.Key()
		if !bytes.HasPrefix(metaKey, pfx) {
			return nil, it.Error()
		}
		_, key, err := DecodeMetaKey(metaKey)
		if err != nil {
			return nil, err
		}
		return key, it.Error()
	}
	return nil, it.Error()
}

func allKeysWithTag(r rpdbReader, db uint32, tag []byte) ([][]byte, error) {
	it := r.getIterator()
	defer r.putIterator(it)
	var keys [][]byte
	pfx := EncodeMetaKeyPrefixTag(db, tag)
	for it.SeekTo(pfx); it.Valid(); it.Next() {
		metaKey := it.Key()
		if !bytes.HasPrefix(metaKey, pfx) {
			break
		}
		_, key, err := DecodeMetaKey(metaKey)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return keys, nil
}
