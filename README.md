# 6.5840 Lab 2: Key/Value Server

## Running Tests (Windows PowerShell)

`make` is a Unix tool and won't work in PowerShell. Use `go test` directly instead.

---

### Step 1 — KV Server, Reliable Network

```powershell
cd E:\CS\CS390\linearizable-kv-store\src\kvsrv1
go test -v -run Reliable
```

Expected passing tests: `TestReliablePut`, `TestPutConcurrentReliable`, `TestMemPutManyClientsReliable`

---

### Step 2 — Lock Implementation, Reliable Network

```powershell
cd E:\CS\CS390\linearizable-kv-store\src\kvsrv1\lock
go test -v -run Reliable
```

Expected passing tests: `TestReliableBasic`, `TestReliableNested`, `TestOneClientReliable`, `TestManyClientsReliable`

---

### Step 3 — KV Server, Unreliable Network

```powershell
cd E:\CS\CS390\linearizable-kv-store\src\kvsrv1
go test -v
```

Expected passing tests: all reliable tests + `TestUnreliableNet`

---

### Step 4 — Lock Implementation, Unreliable Network

```powershell
cd E:\CS\CS390\linearizable-kv-store\src\kvsrv1\lock
go test -v
```

Expected passing tests: all reliable tests + `TestOneClientUnreliable`, `TestManyClientsUnreliable`

---

### Note on `-race`

The `-race` flag requires CGO with a 64-bit GCC compiler. If you see:

```
cc1.exe: sorry, unimplemented: 64-bit mode not compiled in
```

Just omit `-race` — the tests will still run correctly without it. To fix it properly, install a 64-bit GCC via [MSYS2](https://www.msys2.org/).
