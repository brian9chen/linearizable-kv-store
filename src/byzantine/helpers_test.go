package byzantine

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	raft "6.5840/raft1"
	"6.5840/tester1"
)

func init() {
	// Commands in LogEntry.Command are interface{}; gob needs concrete types.
	labgob.Register(0)
}

// endName returns the ClientEnd name used by server from to talk to server to.
func endName(from, to int) string {
	return fmt.Sprintf("e%d-%d", from, to)
}

func serverName(i int) string {
	return fmt.Sprintf("s%d", i)
}

// raftCluster is an in-process Raft group (no tester daemon) for Byzantine tests.
type raftCluster struct {
	tb testing.TB

	n          int
	net        *labrpc.Network
	rfs        []raftapi.Raft
	applyCh    []chan raftapi.ApplyMsg
	persisters []*tester.Persister

	mu        sync.Mutex
	committed []map[int]any // per-server index -> command

	stopDrain chan struct{}
}

func newRaftCluster(tb testing.TB, n int) *raftCluster {
	return newRaftClusterMAC(tb, n, nil)
}

// newRaftClusterMAC enables HMAC-SHA256 on all RPC request bytes when macKey is non-nil.
func newRaftClusterMAC(tb testing.TB, n int, macKey []byte) *raftCluster {
	tb.Helper()
	if n < 1 {
		tb.Fatal("n >= 1")
	}
	net := labrpc.MakeNetwork()
	net.Reliable(true)
	net.LongDelays(false)

	if len(macKey) > 0 {
		m := NewMACManager(macKey)
		net.SetOutboundInterceptor(m.WrapOutbound)
		net.AddInboundInterceptor(m.VerifyInbound)
	}

	c := &raftCluster{
		tb:        tb,
		n:         n,
		net:       net,
		rfs:       make([]raftapi.Raft, n),
		applyCh:   make([]chan raftapi.ApplyMsg, n),
		persisters: make([]*tester.Persister, n),
		committed: make([]map[int]any, n),
		stopDrain: make(chan struct{}),
	}

	for i := 0; i < n; i++ {
		c.applyCh[i] = make(chan raftapi.ApplyMsg, 2048)
		c.persisters[i] = tester.MakePersister()
		c.committed[i] = map[int]any{}

		peers := make([]*labrpc.ClientEnd, n)
		for j := 0; j < n; j++ {
			en := endName(i, j)
			peers[j] = net.MakeEnd(en)
			net.Connect(en, serverName(j))
			net.Enable(en, true)
		}

		rf := raft.Make(peers, i, c.persisters[i], c.applyCh[i])
		c.rfs[i] = rf

		r, ok := rf.(*raft.Raft)
		if !ok {
			tb.Fatalf("unexpected Raft concrete type %T", rf)
		}
		srv := labrpc.MakeServer()
		srv.AddService(labrpc.MakeService(r))
		net.AddServer(serverName(i), srv)

		si := i
		go func() {
			for {
				select {
				case m := <-c.applyCh[si]:
					if !m.CommandValid {
						continue
					}
					c.mu.Lock()
					c.committed[si][m.CommandIndex] = m.Command
					c.mu.Unlock()
				case <-c.stopDrain:
					return
				}
			}
		}()
	}

	return c
}

func (c *raftCluster) cleanup() {
	c.tb.Helper()
	for i := 0; i < c.n; i++ {
		if r, ok := c.rfs[i].(*raft.Raft); ok {
			r.Kill()
		}
	}
	time.Sleep(50 * time.Millisecond)
	// Drain buffered apply messages before stopping drainers.
	for i := 0; i < c.n; i++ {
		for {
			select {
			case m := <-c.applyCh[i]:
				if m.CommandValid {
					c.mu.Lock()
					c.committed[i][m.CommandIndex] = m.Command
					c.mu.Unlock()
				}
			default:
				goto nextCh
			}
		}
	nextCh:
	}
	close(c.stopDrain)
	time.Sleep(10 * time.Millisecond)
	c.net.Cleanup()
}

func (c *raftCluster) waitLeader(timeout time.Duration) (leader int, term int) {
	c.tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := 0
		var lastLeader int
		var lastTerm int
		for i := 0; i < c.n; i++ {
			tm, isLeader := c.rfs[i].GetState()
			if isLeader {
				leaders++
				lastLeader = i
				lastTerm = tm
			}
		}
		if leaders == 1 {
			return lastLeader, lastTerm
		}
		time.Sleep(15 * time.Millisecond)
	}
	c.tb.Fatal("no single leader within timeout")
	return -1, -1
}

// submit tries Start(cmd) on each server until the leader accepts (with retries).
func (c *raftCluster) submit(cmd int, timeout time.Duration) (index int) {
	c.tb.Helper()
	deadline := time.Now().Add(timeout)
	start := 0
	for time.Now().Before(deadline) {
		for k := 0; k < c.n; k++ {
			i := (start + k) % c.n
			idx, _, ok := c.rfs[i].Start(cmd)
			if ok {
				return idx
			}
		}
		time.Sleep(20 * time.Millisecond)
		start++
	}
	c.tb.Fatal("submit failed: no leader accepted command")
	return -1
}

func (c *raftCluster) committedAt(server, index int) (cmd any, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd, ok = c.committed[server][index]
	return cmd, ok
}

func (c *raftCluster) waitCommittedOnAll(index int, timeout time.Duration) {
	c.tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		nok := 0
		for i := 0; i < c.n; i++ {
			if _, ok := c.committedAt(i, index); ok {
				nok++
			}
		}
		if nok == c.n {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	c.tb.Fatalf("index %d not applied on all servers", index)
}

func decodeAppendEntriesArgs(b []byte) (raft.AppendEntriesArgs, error) {
	var a raft.AppendEntriesArgs
	rb := bytes.NewBuffer(b)
	rd := labgob.NewDecoder(rb)
	if err := rd.Decode(&a); err != nil {
		return a, err
	}
	return a, nil
}

func encodeAppendEntriesArgs(a *raft.AppendEntriesArgs) []byte {
	return labrpc.Marshall(a)
}
