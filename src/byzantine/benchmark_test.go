package byzantine

import (
	"testing"
	"time"
)

var benchMACKey = []byte("0123456789abcdef0123456789abcdef")

func BenchmarkHMACSHA256_64B(b *testing.B) {
	m := NewMACManager(benchMACKey)
	payload := make([]byte, 64)
	svc := "Raft.AppendEntries"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.WrapOutbound(nil, svc, payload)
	}
}

func BenchmarkHMACSHA256_1KB(b *testing.B) {
	m := NewMACManager(benchMACKey)
	payload := make([]byte, 1024)
	svc := "Raft.AppendEntries"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.WrapOutbound(nil, svc, payload)
	}
}

func BenchmarkHMACSHA256_8KB(b *testing.B) {
	m := NewMACManager(benchMACKey)
	payload := make([]byte, 8192)
	svc := "Raft.AppendEntries"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.WrapOutbound(nil, svc, payload)
	}
}

func BenchmarkRaftSubmit3Nodes_NoMAC(b *testing.B) {
	c := newRaftCluster(b, 3)
	defer c.cleanup()
	c.waitLeader(5 * time.Second)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.submit(i, 10*time.Second)
	}
}

func BenchmarkRaftSubmit3Nodes_MAC(b *testing.B) {
	c := newRaftClusterMAC(b, 3, benchMACKey)
	defer c.cleanup()
	c.waitLeader(5 * time.Second)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.submit(i, 10*time.Second)
	}
}
