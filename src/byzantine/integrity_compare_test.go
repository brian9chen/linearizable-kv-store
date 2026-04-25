package byzantine

import (
	"testing"
	"time"

	"6.5840/labrpc"
)

// wireFlipFirstNonEmptyByte flips one bit on inbound request bytes (simulates line noise / attacker).
// For MAC tests it must run before VerifyInbound on the receiver.
func wireFlipFirstByte() labrpc.MsgInterceptor {
	return func(_ interface{}, _ string, args []byte) []byte {
		if len(args) < 2 {
			return args
		}
		out := append([]byte(nil), args...)
		out[0] ^= 0x01
		return out
	}
}

// TestCompare_WithoutMAC_vs_WithHMAC groups the two modes side-by-side (use: go test -v -run TestCompare).
//
// Without MAC: this test FAILS the usual safety story — followers can apply a different command than the
// leader at the same log index (so the test asserts that "bad" outcome).
// With HMAC: tampered bytes fail verification and RPCs are dropped, so you do not get quorum commit
// of garbage (the test asserts no unanimous apply under identical wire tampering).
func TestCompare_WithoutMAC_vs_WithHMAC(t *testing.T) {
	t.Run("WithoutMAC_tamper_same_index_different_command", func(t *testing.T) {
		t.Log("Expect: leader applies 42; at least one follower applies != 42 (no integrity on payload).")
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
		// This MUST fail (test goes red) if tampering stops affecting followers — e.g. if someone
		// "fixes" Raft without adding crypto and the cluster accidentally rejects bad payloads.
		if divergent == 0 {
			t.Fatal("FAIL: without MAC, expected follower/leader apply mismatch at same index; got full agreement (demo no longer shows the vulnerability)")
		}
		t.Logf("OK: safety violated without MAC (expected for this demo): %d followers != leader at index %d", divergent, idx)
	})

	t.Run("WithHMAC_wire_flip_dropped_no_unanimous_commit", func(t *testing.T) {
		t.Log("Expect: MAC verify fails after bit-flip; replication cannot complete quorum for that submit.")
		c := newRaftCluster(t, 3)
		defer c.cleanup()
		c.waitLeader(3 * time.Second)

		m := NewMACManager(testSharedKey())
		c.net.SetOutboundInterceptor(m.WrapOutbound)
		c.net.AddInboundInterceptor(wireFlipFirstByte())
		c.net.AddInboundInterceptor(m.VerifyInbound)

		idx := c.submit(77, 10*time.Second)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			all := 0
			for i := 0; i < c.n; i++ {
				if _, ok := c.committedAt(i, idx); ok {
					all++
				}
			}
			if all == c.n {
				t.Fatal("FAIL: with HMAC + wire tamper, did not expect unanimous commit (MAC should drop bad RPCs)")
			}
			time.Sleep(30 * time.Millisecond)
		}
		t.Logf("OK: no unanimous apply at index %d under wire flip + HMAC (expected)", idx)
	})
}
