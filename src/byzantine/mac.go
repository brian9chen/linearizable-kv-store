package byzantine

import (
	"crypto/hmac"
	"crypto/sha256"
)

// MACSize is the HMAC-SHA256 digest length appended to each RPC payload.
const MACSize = sha256.Size

// MACManager attaches HMAC-SHA256(svcMeth, payload) to marshalled RPC args.
// All peers must share the same symmetric key. This authenticates messages
// against tampering (bit flips, payload swaps) but is not full BFT.
type MACManager struct {
	key []byte
}

// NewMACManager copies key into the manager.
func NewMACManager(key []byte) *MACManager {
	k := append([]byte(nil), key...)
	return &MACManager{key: k}
}

func (m *MACManager) computeMAC(svcMeth string, payload []byte) []byte {
	h := hmac.New(sha256.New, m.key)
	h.Write([]byte(svcMeth))
	h.Write([]byte{0})
	h.Write(payload)
	return h.Sum(nil)
}

// WrapOutbound implements labrpc.MsgInterceptor: append MAC after gob args.
func (m *MACManager) WrapOutbound(_ interface{}, svcMeth string, args []byte) []byte {
	mac := m.computeMAC(svcMeth, args)
	out := make([]byte, len(args)+MACSize)
	copy(out, args)
	copy(out[len(args):], mac)
	return out
}

// VerifyInbound implements labrpc.MsgInterceptor: strip and verify MAC.
// Returns nil to signal drop (verification failed or truncated message).
func (m *MACManager) VerifyInbound(_ interface{}, svcMeth string, data []byte) []byte {
	if len(data) < MACSize {
		return nil
	}
	body := data[:len(data)-MACSize]
	got := data[len(data)-MACSize:]
	want := m.computeMAC(svcMeth, body)
	if !hmac.Equal(got, want) {
		return nil
	}
	return body
}
