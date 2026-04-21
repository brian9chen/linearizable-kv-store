// Package byzantine provides toy RPC corruption for demonstrating Raft
// behavior under tampered messages (not full BFT).
package byzantine

import (
	"math/rand"

	"6.5840/labrpc"
)

// FlipRandomBits returns a copy of args with each byte independently corrupted
// with probability rate (0..1): XOR with a random non-zero byte mask.
func FlipRandomBits(args []byte, rate float64) []byte {
	if len(args) == 0 || rate <= 0 {
		return append([]byte(nil), args...)
	}
	out := make([]byte, len(args))
	copy(out, args)
	for i := range out {
		if rand.Float64() < rate {
			out[i] ^= byte(rand.Intn(255) + 1)
		}
	}
	return out
}

// MakeByzantineInterceptor corrupts RPC args only when the sending ClientEnd's
// name is in byzantineEnds. rate is passed to FlipRandomBits.
func MakeByzantineInterceptor(byzantineEnds map[interface{}]bool, rate float64) labrpc.MsgInterceptor {
	return func(endname interface{}, svcMeth string, args []byte) []byte {
		if !byzantineEnds[endname] {
			return args
		}
		return FlipRandomBits(args, rate)
	}
}
