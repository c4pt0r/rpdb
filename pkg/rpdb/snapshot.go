// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package rpdb

import (
	"container/list"
	"sync"
	"time"

	"github.com/wandoulabs/rpdb/pkg/store"
	"github.com/wandoulabs/redis-port/pkg/libs/errors"
	"github.com/wandoulabs/redis-port/pkg/libs/log"
	"github.com/wandoulabs/redis-port/pkg/rdb"
)

var (
	ErrSnapClosed = errors.Static("rpdb snapshot has been closed")
)

type RpdbSnapshot struct {
	mu sync.Mutex
	sp store.Snapshot

	cursor struct {
		it store.Iterator
		sync.Mutex
	}
	readers struct {
		list.List
		sync.Mutex
	}
}

func (s *RpdbSnapshot) acquire() error {
	s.mu.Lock()
	if s.sp != nil {
		return nil
	}
	s.mu.Unlock()
	return errors.Trace(ErrSnapClosed)
}

func (s *RpdbSnapshot) release() {
	s.mu.Unlock()
}

func (s *RpdbSnapshot) Close() {
	if err := s.acquire(); err != nil {
		return
	}
	defer s.release()
	log.Infof("snapshot is closing ...")
	if s.cursor.it != nil {
		s.cursor.it.Close()
		s.cursor.it = nil
	}
	for i := s.readers.Len(); i != 0; i-- {
		r := s.readers.Remove(s.readers.Front()).(*snapshotReader)
		r.cleanup()
	}
	s.sp.Close()
	s.sp = nil
	log.Infof("snapshot is closed")
}

func (s *RpdbSnapshot) getReader() *snapshotReader {
	s.readers.Lock()
	defer s.readers.Unlock()
	if e := s.readers.Front(); e != nil {
		return s.readers.Remove(e).(*snapshotReader)
	}
	return &snapshotReader{sp: s.sp}
}

func (s *RpdbSnapshot) putReader(r *snapshotReader) {
	s.readers.Lock()
	s.readers.PushFront(r)
	s.readers.Unlock()
}

type snapshotReader struct {
	sp store.Snapshot
	it *rpdbIterator
}

func (s *snapshotReader) getRowValue(key []byte) ([]byte, error) {
	return s.sp.Get(key)
}

func (s *snapshotReader) getIterator() (it *rpdbIterator) {
	if s.it != nil {
		it, s.it = s.it, nil
		if it.Error() == nil {
			return it
		}
		it.Close()
	}
	return &rpdbIterator{Iterator: s.sp.NewIterator()}
}

func (s *snapshotReader) putIterator(it *rpdbIterator) {
	if s.it == nil {
		if it.Error() == nil {
			s.it = it
			return
		}
	}
	it.Close()
}

func (s *snapshotReader) cleanup() {
	if s.it != nil {
		s.it.Close()
		s.it = nil
	}
}

func (s *RpdbSnapshot) LoadObjCron(wait time.Duration, ncpu, step int) ([]*rdb.ObjEntry, bool, error) {
	if err := s.acquire(); err != nil {
		return nil, false, err
	}
	defer s.release()

	if wait <= 0 || ncpu <= 0 || step <= 0 {
		return nil, false, errors.Errorf("wait = %d, ncpu = %d, step = %d", wait, ncpu, step)
	}

	ctrl := make(chan int, 0)
	exit := make(chan int, ncpu)
	rets := &struct {
		sync.Mutex
		objs []*rdb.ObjEntry
		more bool
		err  error
	}{}

	var wg sync.WaitGroup
	for i := 0; i < ncpu; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			objs, more, err := s.loadObjCron(ctrl, exit)
			rets.Lock()
			if len(objs) != 0 {
				rets.objs = append(rets.objs, objs...)
			}
			if more {
				rets.more = true
			}
			if rets.err == nil && err != nil {
				rets.err = err
			}
			rets.Unlock()
		}()
	}

	deadline := time.Now().Add(wait)
	for stop := false; !stop && step != 0; step-- {
		select {
		case ctrl <- 0:
		case <-exit:
			stop = true
		}
		if time.Now().After(deadline) {
			stop = true
		}
	}
	close(ctrl)
	wg.Wait()
	return rets.objs, rets.more, rets.err
}

func (s *RpdbSnapshot) scanMetaKey() (metaKey []byte, err error) {
	s.cursor.Lock()
	defer s.cursor.Unlock()
	it := s.cursor.it
	if it == nil {
		it = s.sp.NewIterator()
		it.SeekTo([]byte{MetaCode})
		s.cursor.it = it
	}
	if !it.Valid() {
		return nil, it.Error()
	}
	metaKey = it.Key()
	it.Next()
	if len(metaKey) != 0 && metaKey[0] != MetaCode {
		metaKey = nil
	}
	return metaKey, it.Error()
}

func (s *RpdbSnapshot) loadObjCron(ctrl <-chan int, exit chan<- int) (objs []*rdb.ObjEntry, more bool, err error) {
	r := s.getReader()
	defer s.putReader(r)
	defer func() {
		exit <- 0
	}()
	for {
		if _, ok := <-ctrl; !ok {
			return objs, more, nil
		}
		metaKey, err := s.scanMetaKey()
		if err != nil {
			return nil, false, err
		}
		if metaKey == nil {
			return objs, more, nil
		}
		more = true

		db, key, err := DecodeMetaKey(metaKey)
		if err != nil {
			return nil, false, err
		}
		_, obj, err := loadObjEntry(r, db, key)
		if err != nil {
			return nil, false, err
		}
		if obj != nil {
			objs = append(objs, obj)
		}
	}
}

func (s *RpdbSnapshot) loadRowCron(ctrl <-chan int, exit chan<- int) (rows []rpdbRow, more bool, err error) {
	r := s.getReader()
	defer s.putReader(r)
	defer func() {
		exit <- 0
	}()
	for {
		if _, ok := <-ctrl; !ok {
			return rows, more, nil
		}
		metaKey, err := s.scanMetaKey()
		if err != nil {
			return nil, false, err
		}
		if metaKey == nil {
			return rows, more, nil
		}
		more = true

		db, key, err := DecodeMetaKey(metaKey)
		if err != nil {
			return nil, false, err
		}
		o, err := loadRpdbRow(r, db, key)
		if err != nil {
			return nil, false, err
		}
		if o != nil {
			rows = append(rows, o)
		}
	}
}
