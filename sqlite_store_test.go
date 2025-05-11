package raftsqlite

import (
	"fmt"
	"testing"

	"github.com/hashicorp/raft"
)

func mustSqliteInMemoryStore(t testing.TB) *SqliteStore {
	store, err := NewStore("file::memory:?cache=shared")
	assertNoError(t, err)

	return store
}

func mustSqliteDiskStore(t testing.TB) *SqliteStore {
	tempdir := t.TempDir()
	store, err := NewStore(tempdir + "/raft.db")
	assertNoError(t, err)

	return store
}

func createRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func assert(t testing.TB, b bool, msg string) {
	t.Helper()
	if !b {
		t.Fatal(msg)
	}
}

func assertNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}
}

func TestImplementsStoreInterface(t *testing.T) {
	var store interface{} = &SqliteStore{}
	_, ok := store.(raft.StableStore)
	assert(t, ok, "SqliteStore does not implement raft.StableStore")
	_, ok = store.(raft.LogStore)
	assert(t, ok, "SqliteStore does not implement raft.LogStore")
}

func TestSqliteJounalModeInMemory(t *testing.T) {
	store := mustSqliteInMemoryStore(t)
	defer store.Close()

	// PRAGMA journal_mode=memory
	var journalMode string
	err := store.db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	t.Log(journalMode)
	assert(t, journalMode == "memory", "journal_mode should be memory")
}

func TestSqliteDBIsInitialized(t *testing.T) {
	store := mustSqliteDiskStore(t)
	defer func() {
		store.Close()
		store.deleteDB()
	}()

	// logs table was created
	var count int
	err := store.db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='logs'").Scan(&count)
	assertNoError(t, err)
	assert(t, count == 1, "logs table should exist")

	// kv table was created
	err = store.db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='kv'").Scan(&count)
	assertNoError(t, err)
	assert(t, count == 1, "kv table should exist")

	// PRAGMA journal_mode=WAL
	var journalMode string
	err = store.db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	assert(t, journalMode == "wal", "journal_mode should be WAL")

	// PRAGMA synchronous=NORMAL
	var synchronous string
	err = store.db.QueryRow("PRAGMA synchronous").Scan(&synchronous)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	// 1 == NORMAL
	assert(t, synchronous == "1", "synchronous should be normal")
}

func TestStoreLog(t *testing.T) {
	store := mustSqliteDiskStore(t)
	defer func() {
		store.Close()
		store.deleteDB()
	}()

	err := store.StoreLog(createRaftLog(1, "log1"))
	assertNoError(t, err)

	log := new(raft.Log)
	err = store.GetLog(1, log)
	assertNoError(t, err)
	assert(t, log.Index == 1, fmt.Sprintf("want index 1, got: %d", log.Index))
}

func TestFirstIndex(t *testing.T) {
	store := mustSqliteDiskStore(t)
	defer func() {
		store.Close()
		store.deleteDB()
	}()

	idx, err := store.FirstIndex()
	assertNoError(t, err)
	assert(t, idx == 0, fmt.Sprintf("want 0, got: %d", idx))

	logs := []*raft.Log{
		createRaftLog(1, "log1"),
		createRaftLog(2, "log2"),
		createRaftLog(3, "log3"),
	}
	err = store.StoreLogs(logs)
	assertNoError(t, err)

	idx, err = store.FirstIndex()
	assertNoError(t, err)
	assert(t, idx == 1, fmt.Sprintf("want first index 1, got: %d", idx))
}

func TestLastIndex(t *testing.T) {
	store := mustSqliteDiskStore(t)
	defer func() {
		store.Close()
		store.deleteDB()
	}()

	logs := []*raft.Log{
		createRaftLog(1, "log1"),
		createRaftLog(2, "log2"),
		createRaftLog(3, "log3"),
	}
	err := store.StoreLogs(logs)
	assertNoError(t, err)

	idx, err := store.LastIndex()
	assertNoError(t, err)
	assert(t, idx == 3, fmt.Sprintf("want last index 3, got: %d", idx))
}

func TestGetLog(t *testing.T) {
	store := mustSqliteDiskStore(t)
	defer func() {
		store.Close()
		store.deleteDB()
	}()

	log := new(raft.Log)

	err := store.GetLog(1, log)
	assert(t, err == raft.ErrLogNotFound, fmt.Sprintf("want log not found, got: %s", err))

	logs := []*raft.Log{
		createRaftLog(1, "log1"),
		createRaftLog(2, "log2"),
		createRaftLog(3, "log3"),
	}
	err = store.StoreLogs(logs)
	assertNoError(t, err)

	err = store.GetLog(2, log)
	assertNoError(t, err)
	assert(t, log.Index == 2, fmt.Sprintf("want index 2, got: %d", log.Index))
}

func TestDeleteRange(t *testing.T) {
	store := mustSqliteDiskStore(t)
	defer func() {
		store.Close()
		store.deleteDB()
	}()

	logs := []*raft.Log{
		createRaftLog(1, "log1"),
		createRaftLog(2, "log2"),
		createRaftLog(3, "log3"),
	}
	err := store.StoreLogs(logs)
	assertNoError(t, err)

	err = store.DeleteRange(1, 2)
	assertNoError(t, err)

	log := new(raft.Log)
	err = store.GetLog(1, log)
	assert(t, err == raft.ErrLogNotFound, fmt.Sprintf("want log not found, got: %s", err))
	err = store.GetLog(2, log)
	assert(t, err == raft.ErrLogNotFound, fmt.Sprintf("want log not found, got: %s", err))

	err = store.GetLog(3, log)
	assertNoError(t, err)
	assert(t, log.Index == 3, fmt.Sprintf("want index 3, got: %d", log.Index))
}

func TestSetGet(t *testing.T) {
	store := mustSqliteDiskStore(t)
	defer func() {
		store.Close()
		store.deleteDB()
	}()

	err := store.Set([]byte("key1"), []byte("val1"))
	assertNoError(t, err)

	val, err := store.Get([]byte("key1"))
	assertNoError(t, err)
	assert(t, string(val) == "val1", fmt.Sprintf("want val1, got: %s", val))
}

func TestSetGetUint64(t *testing.T) {
	store := mustSqliteDiskStore(t)
	defer func() {
		store.Close()
		store.deleteDB()
	}()

	err := store.SetUint64([]byte("key1"), 123)
	assertNoError(t, err)

	val, err := store.GetUint64([]byte("key1"))
	assertNoError(t, err)
	assert(t, val == 123, fmt.Sprintf("want 123, got: %d", val))

	_, err = store.GetUint64([]byte("404"))
	assert(t, err == ErrKeyNotFound, fmt.Sprintf("want not found err, got: %s", err))
}
