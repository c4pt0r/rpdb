package boltdb

import (
	"fmt"
	"os"
	"path"

	"github.com/boltdb/bolt"
	"github.com/wandoulabs/rpdb/pkg/store"
)

var bucketName = []byte("rpdb")

type BoltDB struct {
	path string
	db   *bolt.DB
	cfg  *Config
}

func Open(dbPath string, conf *Config, create, repair bool) (*BoltDB, error) {
	os.MkdirAll(dbPath, 0755)
	name := path.Join(dbPath, "rpdb_bolt.db")
	db := &BoltDB{
		path: dbPath,
		cfg:  conf,
	}
	var err error

	db.db, err = bolt.Open(name, 0600, nil)
	if err != nil {
		return nil, err
	}

	var tx *bolt.Tx
	tx, err = db.db.Begin(true)
	if err != nil {
		return nil, err
	}

	_, err = tx.CreateBucketIfNotExists(bucketName)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *BoltDB) Close() {
	db.db.Close()
}

func (db *BoltDB) Clear() error {
	err := db.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(bucketName); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketName); err != nil {
			return err
		}
		return nil
	})
	return err
}

func (db *BoltDB) Get(key []byte) ([]byte, error) {
	var value []byte

	t, err := db.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer t.Rollback()

	b := t.Bucket(bucketName)

	value = b.Get(key)

	if value == nil {
		return nil, nil
	} else {
		return append([]byte{}, value...), nil
	}
}

func (db *BoltDB) NewIterator() store.Iterator {
	tx, err := db.db.Begin(false)
	if err != nil {
		return &Iterator{}
	}
	b := tx.Bucket(bucketName)

	return &Iterator{
		tx: tx,
		it: b.Cursor()}
}

func (db *BoltDB) Commit(bt *store.Batch) error {
	err := db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		var err error
		for e := bt.OpList.Front(); e != nil; e = e.Next() {
			switch op := e.Value.(type) {
			case *store.BatchOpSet:
				err = b.Put(op.Key, op.Value)
			case *store.BatchOpDel:
				err = b.Delete(op.Key)
			default:
				panic(fmt.Sprintf("unsupported batch operation: %+v", op))
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (db *BoltDB) NewSnapshot() store.Snapshot {
	return newSnapshot(db)
}

func (db *BoltDB) Compact(start, limit []byte) error {
	return nil
}

func (db *BoltDB) Stats() string {
	return ""
}
