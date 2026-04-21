package byzantine

import (
	"testing"
	"time"

	"6.5840/labrpc"
)

// testSharedKey is 32 bytes (typical AES/HMAC key size for demos).
func testSharedKey() []byte {
	return []byte("0123456789abcdef0123456789abcdef")
}

func TestMACRoundTrip(t *testing.T) {
	key := testSharedKey()
	m := NewMACManager(key)
	svc := "Raft.AppendEntries"
	payload := []byte{1, 2, 3, 4, 5}
	wire := m.WrapOutbound(nil, svc, payload)
	got := m.VerifyInbound(nil, svc, wire)
	if string(got) != string(payload) {
		t.Fatalf("payload mismatch: got %v want %v", got, payload)
	}
}

func TestMACRejectsWrongKey(t *testing.T) {
	m1 := NewMACManager(testSharedKey())
	m2 := NewMACManager([]byte("wrong-key-32bytes-abcdefghijklmnop"))
	svc := "Raft.AppendEntries"
	payload := []byte{9, 9, 9}
	wire := m1.WrapOutbound(nil, svc, payload)
	if m2.VerifyInbound(nil, svc, wire) != nil {
		t.Fatal("expected verification failure with wrong key")
	}
}

func TestMACRejectsTampering(t *testing.T) {
	m := NewMACManager(testSharedKey())
	svc := "Raft.AppendEntries"
	wire := m.WrapOutbound(nil, svc, []byte("hello"))
	if len(wire) < 2 {
		t.Fatal("wire too short")
	}
	wire[len(wire)-10] ^= 0x01
	if m.VerifyInbound(nil, svc, wire) != nil {
		t.Fatal("expected verification failure after tampering")
	}
}

func TestMACRejectsWrongMethod(t *testing.T) {
	m := NewMACManager(testSharedKey())
	wire := m.WrapOutbound(nil, "Raft.AppendEntries", []byte("x"))
	if m.VerifyInbound(nil, "Raft.RequestVote", wire) != nil {
		t.Fatal("MAC must bind to method name")
	}
}

// TestMACClusterAgreement runs the same workload as TestBaselineAgreementNoCorruption
// with HMAC on every RPC; cluster should still agree.
func TestMACClusterAgreement(t *testing.T) {
	c := newRaftClusterMAC(t, 3, testSharedKey())
	defer c.cleanup()

	c.waitLeader(4 * time.Second)
	idx := c.submit(42, 6*time.Second)
	c.waitCommittedOnAll(idx, 5*time.Second)

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

// TestMACDropsSingleBitCorruption installs MAC, then flips one bit on the wire
// (before VerifyInbound). Tampered RPCs are dropped; replication cannot complete.
func TestMACDropsSingleBitCorruption(t *testing.T) {
	c := newRaftCluster(t, 3)
	defer c.cleanup()

	c.waitLeader(3 * time.Second)

	m := NewMACManager(testSharedKey())
	c.net.SetOutboundInterceptor(m.WrapOutbound)
	c.net.AddInboundInterceptor(func(_ interface{}, svcMeth string, args []byte) []byte {
		if len(args) < 2 {
			return args
		}
		out := append([]byte(nil), args...)
		out[0] ^= 0x01
		return out
	})
	c.net.AddInboundInterceptor(m.VerifyInbound)

	idx := c.submit(100, 10*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		all := 0
		for i := 0; i < c.n; i++ {
			if _, ok := c.committedAt(i, idx); ok {
				all++
			}
		}
		if all == c.n {
			t.Fatal("did not expect unanimous commit when every inbound RPC is bit-flipped before MAC verify")
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Log("MAC correctly dropped corrupted RPCs; no unanimous apply (expected)")
}

func TestMACManagerMsgInterceptorShape(t *testing.T) {
	m := NewMACManager(testSharedKey())
	var _ labrpc.MsgInterceptor = m.WrapOutbound
	var _ labrpc.MsgInterceptor = m.VerifyInbound
}
