package byzantine

import (
	"strings"
	"testing"
)

// TestSummary_SecurityMatrix prints a compact reference table when run with -v.
//	go test -v ./byzantine -run TestSummary
func TestSummary_SecurityMatrix(t *testing.T) {
	if !testing.Verbose() {
		t.Skip(`readable table: go test -v ./byzantine -run TestSummary`)
	}

	t.Log(strings.TrimSpace(`
--- Byzantine demo (what the suite shows) ---

  Mode         Attack on AppendEntries bytes              Typical outcome
  ------------ ---------------------------------------- -------------------------------
  Plain Raft   Change replicated cmd (42 -> 999)        Leader applies 42; follower(s) 999
  + HMAC       Same logical attack after MAC on wire    Verify fails -> RPC dropped -> no bad commit

  If TestCompare/WithoutMAC_* ever passes agreement after tamper, the vulnerability demo is broken.
  If WithHMAC_* allows unanimous commit after wire flip before verify, MAC wiring is broken.

--- Nicer output options ---

  Human-readable tests:
    ./scripts/byzantine-results.sh
    go test -v ./byzantine -run 'TestCompare|TestByzantine|TestMAC|TestBaseline|TestSummary'

  JSON (CI / tooling):
    go test -json ./byzantine 2>&1 | jq -c 'select(.Action=="pass" or .Action=="fail") | {test: .Test, action: .Action}'

  Benchmarks with alloc counts:
    go test -bench=. -benchmem -benchtime=500ms ./byzantine -run '^$'

  Statistical comparison (after two benchmark runs saved to old.txt new.txt):
    go install golang.org/x/perf/cmd/benchstat@latest
    benchstat old.txt new.txt
`))
}
