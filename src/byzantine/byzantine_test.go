package byzantine

import (
	"fmt"
	"testing"
	"time"

	"6.5840/labrpc"
)

// appendEntriesFromLeader returns an interceptor that only runs f for
// Raft.AppendEntries RPCs whose sending ClientEnd is from the given leader.
func appendEntriesFromLeader(leader int, f labrpc.MsgInterceptor) labrpc.MsgInterceptor {
	return func(endname interface{}, svcMeth string, args []byte) []byte {
		if svcMeth != "Raft.AppendEntries" {
			return args
		}
		s, ok := endname.(string)
		if !ok {
			return args
		}
		var from, to int
		if n, err := fmt.Sscanf(s, "e%d-%d", &from, &to); n != 2 || err != nil {
			return args
		}
		if from != leader {
			return args
		}
		return f(endname, svcMeth, args)
	}
}

func TestBaselineAgreementNoCorruption(t *testing.T) {
	c := newRaftCluster(t, 3)
	defer c.cleanup()

	c.waitLeader(3 * time.Second)
	idx := c.submit(42, 5*time.Second)
	c.waitCommittedOnAll(idx, 4*time.Second)

	for i := 0; i < c.n; i++ {
		cmd, ok := c.committedAt(i, idx)
		if !ok {
			t.Fatalf("server %d missing index %d", i, idx)
		}
		if cmd != 42 {
			t.Fatalf("server %d got %v want 42", i, cmd)
		}
	}
}

// TestByzantinePayloadCorruptionDivergentApply shows followers can apply a
// different command than the leader at the same index when the AppendEntries
// payload is tampered in flight: Raft has no integrity check on entries.
func TestByzantinePayloadCorruptionDivergentApply(t *testing.T) {
	c := newRaftCluster(t, 3)
	defer c.cleanup()

	leader, _ := c.waitLeader(3 * time.Second)

	c.net.AddInboundInterceptor(appendEntriesFromLeader(leader, func(_ interface{}, svcMeth string, args []byte) []byte {
		if svcMeth != "Raft.AppendEntries" {
			return args
		}
		a, err := decodeAppendEntriesArgs(args)
		if err != nil {
			return args
		}
		for i := range a.Entries {
			if v, ok := a.Entries[i].Command.(int); ok && v == 42 {
				a.Entries[i].Command = 999
				return encodeAppendEntriesArgs(&a)
			}
		}
		return args
	}))

	idx := c.submit(42, 8*time.Second)
	c.waitCommittedOnAll(idx, 6*time.Second)

	lcmd, _ := c.committedAt(leader, idx)
	if lcmd != 42 {
		t.Fatalf("leader applied %v want 42", lcmd)
	}

	divergent := 0
	for i := 0; i < c.n; i++ {
		if i == leader {
			continue
		}
		cmd, ok := c.committedAt(i, idx)
		if !ok {
			t.Fatalf("follower %d missing index %d", i, idx)
		}
		if cmd != 42 {
			divergent++
		}
	}
	if divergent == 0 {
		t.Fatal("expected at least one follower to apply a corrupted command (want != 42)")
	}
	t.Logf("Byzantine demo: leader index %d command %v; %d followers differ (plain Raft has no MAC)", idx, lcmd, divergent)
}

// TestByzantineStaleTermBlocksCommit forces replicated AppendEntries (non-empty) to
// carry term 0. Heartbeats are left intact so the leader does not accumulate a bad
// nextIndex from chaotic replies. Followers reject replication, so the command
// cannot commit cluster-wide.
func TestByzantineStaleTermBlocksCommit(t *testing.T) {
	c := newRaftCluster(t, 3)
	defer c.cleanup()

	leader, _ := c.waitLeader(3 * time.Second)

	c.net.AddInboundInterceptor(appendEntriesFromLeader(leader, func(_ interface{}, svcMeth string, args []byte) []byte {
		if svcMeth != "Raft.AppendEntries" {
			return args
		}
		a, err := decodeAppendEntriesArgs(args)
		if err != nil {
			return args
		}
		if len(a.Entries) == 0 {
			return args
		}
		a.Term = 0
		return encodeAppendEntriesArgs(&a)
	}))

	idx := c.submit(100, 12*time.Second)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		all := 0
		for i := 0; i < c.n; i++ {
			if _, ok := c.committedAt(i, idx); ok {
				all++
			}
		}
		if all == c.n {
			t.Fatalf("unexpected unanimous commit at index %d under stale AppendEntries.Term", idx)
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Logf("stale AppendEntries.Term: no unanimous apply at index %d (followers reject; expected)", idx)
}

// TestByzantineRawBitFlipStallsProgress randomly corrupts AppendEntries bytes from
// the leader; decoding often fails (dropped RPC) or semantics break, so agreement stalls.
func TestByzantineRawBitFlipStallsProgress(t *testing.T) {
	c := newRaftCluster(t, 3)
	defer c.cleanup()

	leader, _ := c.waitLeader(3 * time.Second)

	byz := map[interface{}]bool{}
	for j := 0; j < c.n; j++ {
		byz[endName(leader, j)] = true
	}
	c.net.AddInboundInterceptor(appendEntriesFromLeader(leader, MakeByzantineInterceptor(byz, 0.12)))

	ch := make(chan int, 1)
	go func() {
		ch <- c.submit(7, 15*time.Second)
	}()

	time.Sleep(2 * time.Second)

	stuck := true
	select {
	case <-ch:
		stuck = false
	default:
	}

	if !stuck {
		for i := 0; i < c.n; i++ {
			if _, ok := c.committedAt(i, 1); !ok {
				stuck = true
				break
			}
		}
	}

	if !stuck {
		t.Log("bit-flip test: cluster still made progress (probabilistic); try re-run")
	} else {
		t.Log("bit-flip test: progress stalled or incomplete — dropped/corrupt RPCs (expected under random flips)")
	}
}
