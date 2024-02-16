package raftsqlite

import (
	"testing"

	"github.com/hashicorp/raft"
	raftbench "github.com/hashicorp/raft/bench"
)

func benchRunLog(b *testing.B, f func(*testing.B, raft.LogStore)) {
	store := mustSqliteDiskStore(b)
	defer func() {
		store.Close()
		store.deleteDB()
	}()

	inmem := mustSqliteInMemoryStore(b)
	defer inmem.Close()

	b.Run("disk", func(b *testing.B) {
		f(b, store)
	})

	b.Run("memory", func(b *testing.B) {
		f(b, inmem)
	})
}

func benchRunStable(b *testing.B, f func(*testing.B, raft.StableStore)) {
	store := mustSqliteDiskStore(b)
	defer func() {
		store.Close()
		store.deleteDB()
	}()

	inmem := mustSqliteInMemoryStore(b)
	defer inmem.Close()

	b.Run("disk", func(b *testing.B) {
		f(b, store)
	})

	b.Run("memory", func(b *testing.B) {
		f(b, inmem)
	})
}

func BenchmarkFirstIndex(b *testing.B) {
	benchRunLog(b, raftbench.FirstIndex)
}

func BenchmarkLastIndex(b *testing.B) {
	benchRunLog(b, raftbench.LastIndex)
}

func BenchmarkGetLog(b *testing.B) {
	benchRunLog(b, raftbench.GetLog)
}

func BenchmarkStoreLog(b *testing.B) {
	benchRunLog(b, raftbench.StoreLog)
}

func BenchmarkStoreLogs(b *testing.B) {
	benchRunLog(b, raftbench.StoreLogs)
}

func BenchmarkDeleteRange(b *testing.B) {
	benchRunLog(b, raftbench.DeleteRange)
}

func BenchmarkSet(b *testing.B) {
	benchRunStable(b, raftbench.Set)
}

func BenchmarkGet(b *testing.B) {
	benchRunStable(b, raftbench.Get)
}

func BenchmarkSetUint64(b *testing.B) {
	benchRunStable(b, raftbench.SetUint64)
}

func BenchmarkGetUint64(b *testing.B) {
	benchRunStable(b, raftbench.GetUint64)
}
