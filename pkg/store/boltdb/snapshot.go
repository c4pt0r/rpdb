package boltdb

import (
	"github.com/boltdb/bolt"
	"github.com/wandoulabs/rpdb/pkg/store"
)

type Snapshot struct {
	tx *bolt.Tx
	b  *bolt.Bucket
}

func newSnapshot(db *BoltDB) *Snapshot {
	tx, err := db.db.Begin(false)
	if err != nil {
		return nil
	}

	return &Snapshot{
		tx: tx,
		b:  tx.Bucket(bucketName)}
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	return s.b.Get(key), nil
}

func (s *Snapshot) NewIterator() store.Iterator {
	return &Iterator{
		tx: nil,
		it: s.b.Cursor(),
	}
}

func (s *Snapshot) Close() {
	s.tx.Rollback()
}
