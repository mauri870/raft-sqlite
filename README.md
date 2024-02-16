# raft-sqlite

This is a sqlite backend for [hashicorp/raft](https://github.com/hashicorp/raft).

> NOTE: this is slower than a proper key-value backend, in fact it's not recommended to use this in production if you are looking for performance. Stick to the default `raft-boltdb` or `raft-mdb` backends.

## Usage

```bash
go get -u github.com/mauri870/raft-sqlite
```

```go
//...
sqliteStore, err := raftsqlite.NewStore(filepath.Join(raftDir, "raft.db"))
//...
logStore := sqliteStore
stableStore := sqliteStore
_,_ := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
```

It is also possible to use the in-memory sqlite store:

```go
sqliteStore, err := raftsqlite.NewStore("file::memory:?cache=shared")
```

> NOTE: Never use just ":memory:", as it will create a new database for each connection instead of a shared database.

