package byzantine

import (
	"testing"
)

func TestFlipRandomBitsEmpty(t *testing.T) {
	b := FlipRandomBits(nil, 1)
	if len(b) != 0 {
		t.Fatal("expected empty")
	}
	b = FlipRandomBits([]byte{}, 1)
	if len(b) != 0 {
		t.Fatal("expected empty")
	}
}

func TestFlipRandomBitsDeterministicLength(t *testing.T) {
	in := []byte{1, 2, 3, 4, 5}
	out := FlipRandomBits(in, 1.0)
	if len(out) != len(in) {
		t.Fatalf("len %d want %d", len(out), len(in))
	}
}

func TestMakeByzantineInterceptorSkipsUnknownEnd(t *testing.T) {
	bad := map[interface{}]bool{"evil": true}
	f := MakeByzantineInterceptor(bad, 1.0)
	args := []byte{0xff, 0xff}
	got := f("good", "Raft.AppendEntries", args)
	if string(got) != string(args) {
		t.Fatal("should not corrupt unknown end")
	}
}

func TestMakeByzantineInterceptorCorruptsListedEnd(t *testing.T) {
	bad := map[interface{}]bool{"evil": true}
	f := MakeByzantineInterceptor(bad, 1.0)
	args := make([]byte, 32)
	for i := range args {
		args[i] = 0xaa
	}
	got := f("evil", "Raft.AppendEntries", args)
	same := true
	for i := range args {
		if got[i] != args[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("expected some byte to differ at rate 1.0")
	}
}
