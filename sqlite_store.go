package raftsqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/hashicorp/raft"

	_ "github.com/mattn/go-sqlite3"
)

var (
	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

// SqliteStore provides a raft.LogStore to store and retrieve Raft log
// entries from a sqlite database. It also provides a raft.StableStore
// for storage of key/value pairs.
type SqliteStore struct {
	// db is the underlying handle to the sql.DB
	db *sql.DB

	// The path to the database file. This may contain :memory: if the
	// database is in-memory.
	path string
}

// NewStore takes a file path and returns a connected Raft backend.
func NewStore(path string) (*SqliteStore, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err

	}

	store := &SqliteStore{
		db:   db,
		path: path,
	}

	// database initialization
	err = store.transaction(func(tx *sql.Tx) error {
		// Synchronous=full is the default, but normal when paired with
		// WAL mode complete database integrity is guaranteed. Normal
		// also issues less fsyncs.
		_, err := db.Exec("PRAGMA synchronous=normal")
		if err != nil {
			return err
		}

		_, err = db.Exec("PRAGMA journal_mode=WAL")
		if err != nil {
			return err
		}

		_, err = db.Exec("CREATE TABLE IF NOT EXISTS logs (idx INTEGER PRIMARY KEY, data BLOB)")
		if err != nil {
			return err
		}

		_, err = db.Exec("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value BLOB)")
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return store, nil
}

func (s *SqliteStore) transaction(f func(*sql.Tx) error) (err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	err = f(tx)
	if err == nil {
		return tx.Commit()
	}

	txerr := tx.Rollback()
	if txerr == nil {
		return err
	}
	return errors.Join(err, fmt.Errorf("rollback failed: %w", txerr))
}

func (s *SqliteStore) deleteDB() error {
	s.Close()
	return os.Remove(s.path)
}

// Close is used to gracefully close the DB connection.
func (s *SqliteStore) Close() error {
	return s.db.Close()
}

// FirstIndex returns the first known index from the Raft log.
func (s *SqliteStore) FirstIndex() (uint64, error) {
	var idx uint64
	err := s.transaction(func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT idx FROM logs ORDER BY idx ASC LIMIT 1")
		return row.Scan(&idx)
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return idx, nil
}

// LastIndex returns the last known index from the Raft log.
func (s *SqliteStore) LastIndex() (uint64, error) {
	var idx uint64
	err := s.transaction(func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT idx FROM logs ORDER BY idx DESC LIMIT 1")
		return row.Scan(&idx)
	})
	if err != nil {
		return 0, err
	}
	return idx, nil
}

// GetLog is used to retrieve a log at a given index.
func (s *SqliteStore) GetLog(idx uint64, log *raft.Log) error {
	var data []byte
	err := s.transaction(func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT data FROM logs WHERE idx = ?", idx)
		return row.Scan(&data)
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return raft.ErrLogNotFound
		}
		return err
	}

	return decodeMsgPack(data, log)
}

// StoreLog is used to store a single raft log
func (s *SqliteStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (s *SqliteStore) StoreLogs(logs []*raft.Log) error {
	return s.transaction(func(tx *sql.Tx) error {
		for _, log := range logs {
			key := log.Index
			val, err := encodeMsgPack(log)
			if err != nil {
				return err
			}

			_, err = tx.Exec("INSERT INTO logs (idx, data) VALUES (?, ?)", key, val.Bytes())
		}
		return nil
	})
}

// DeleteRange is used to delete logs within a given range inclusively.
func (s *SqliteStore) DeleteRange(min, max uint64) error {
	return s.transaction(func(tx *sql.Tx) error {
		_, err := tx.Exec("DELETE FROM logs WHERE idx >= ? AND idx <= ?", min, max)
		return err
	})
}

// Set is used to set a key/value set outside of the raft log
func (s *SqliteStore) Set(k, v []byte) error {
	return s.transaction(func(tx *sql.Tx) error {
		_, err := tx.Exec("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", k, v)
		return err
	})
}

// Get is used to retrieve a value from the k/v store by key
func (s *SqliteStore) Get(k []byte) ([]byte, error) {
	var value []byte
	err := s.transaction(func(tx *sql.Tx) error {
		row := tx.QueryRow("SELECT value FROM kv WHERE key = ?", k)
		return row.Scan(&value)
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	return value, nil
}

// SetUint64 is like Set, but handles uint64 values
func (s *SqliteStore) SetUint64(key []byte, val uint64) error {
	return s.Set(key, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (s *SqliteStore) GetUint64(key []byte) (uint64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}
