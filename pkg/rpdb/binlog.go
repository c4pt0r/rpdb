// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package rpdb

import (
	"container/list"
	"sync"

	"github.com/wandoulabs/rpdb/pkg/store"
	"github.com/wandoulabs/redis-port/pkg/libs/errors"
	"github.com/wandoulabs/redis-port/pkg/libs/log"
)

var (
	ErrClosed = errors.Static("rpdb has been closed")
)

type Rpdb struct {
	mu sync.Mutex
	db store.Database

	splist list.List
	itlist list.List
	serial uint64
}

func New(db store.Database) *Rpdb {
	return &Rpdb{db: db}
}

func (b *Rpdb) acquire() error {
	b.mu.Lock()
	if b.db != nil {
		return nil
	}
	b.mu.Unlock()
	return errors.Trace(ErrClosed)
}

func (b *Rpdb) release() {
	b.mu.Unlock()
}

func (b *Rpdb) commit(bt *store.Batch, fw *Forward) error {
	if bt.Len() == 0 {
		return nil
	}
	if err := b.db.Commit(bt); err != nil {
		log.WarnErrorf(err, "rpdb commit failed")
		return err
	}
	for i := b.itlist.Len(); i != 0; i-- {
		v := b.itlist.Remove(b.itlist.Front()).(*rpdbIterator)
		v.Close()
	}
	b.serial++
	return nil
}

func (b *Rpdb) getRowValue(key []byte) ([]byte, error) {
	return b.db.Get(key)
}

func (b *Rpdb) getIterator() (it *rpdbIterator) {
	if e := b.itlist.Front(); e != nil {
		return b.itlist.Remove(e).(*rpdbIterator)
	}
	return &rpdbIterator{
		Iterator: b.db.NewIterator(),
		serial:   b.serial,
	}
}

func (b *Rpdb) putIterator(it *rpdbIterator) {
	if it.serial == b.serial && it.Error() == nil {
		b.itlist.PushFront(it)
	} else {
		it.Close()
	}
}

func (b *Rpdb) Close() {
	if err := b.acquire(); err != nil {
		return
	}
	defer b.release()
	log.Infof("rpdb is closing ...")
	for i := b.splist.Len(); i != 0; i-- {
		v := b.splist.Remove(b.splist.Front()).(*RpdbSnapshot)
		v.Close()
	}
	for i := b.itlist.Len(); i != 0; i-- {
		v := b.itlist.Remove(b.itlist.Front()).(*rpdbIterator)
		v.Close()
	}
	if b.db != nil {
		b.db.Close()
		b.db = nil
	}
	log.Infof("rpdb is closed")
}

func (b *Rpdb) NewSnapshot() (*RpdbSnapshot, error) {
	if err := b.acquire(); err != nil {
		return nil, err
	}
	defer b.release()
	sp := &RpdbSnapshot{sp: b.db.NewSnapshot()}
	b.splist.PushBack(sp)
	log.Infof("rpdb create new snapshot, address = %p", sp)
	return sp, nil
}

func (b *Rpdb) ReleaseSnapshot(sp *RpdbSnapshot) {
	if err := b.acquire(); err != nil {
		return
	}
	defer b.release()
	log.Infof("rpdb release snapshot, address = %p", sp)
	for i := b.splist.Len(); i != 0; i-- {
		v := b.splist.Remove(b.splist.Front()).(*RpdbSnapshot)
		if v != sp {
			b.splist.PushBack(v)
		}
	}
	sp.Close()
}

func (b *Rpdb) Reset() error {
	if err := b.acquire(); err != nil {
		return err
	}
	defer b.release()
	log.Infof("rpdb is reseting...")
	for i := b.splist.Len(); i != 0; i-- {
		v := b.splist.Remove(b.splist.Front()).(*RpdbSnapshot)
		v.Close()
	}
	for i := b.itlist.Len(); i != 0; i-- {
		v := b.itlist.Remove(b.itlist.Front()).(*rpdbIterator)
		v.Close()
	}
	if err := b.db.Clear(); err != nil {
		b.db.Close()
		b.db = nil
		log.ErrorErrorf(err, "rpdb reset failed")
		return err
	} else {
		b.serial++
		log.Infof("rpdb is reset")
		return nil
	}
}

func (b *Rpdb) compact(start, limit []byte) error {
	if err := b.db.Compact(start, limit); err != nil {
		log.ErrorErrorf(err, "rpdb compact failed")
		return err
	} else {
		return nil
	}
}

func errArguments(format string, v ...interface{}) error {
	err := errors.Errorf(format, v...)
	log.DebugErrorf(err, "call rpdb function with invalid arguments")
	return err
}
