package node

import (
	"encoding/json"
	"testing"
)

func TestNodeStatusRequestRoundTrip(t *testing.T) {
	if OpNodeStatus != OpAckBatch+1 {
		t.Fatalf("OpNodeStatus = %d, want %d: operation values are stable on the wire", OpNodeStatus, OpAckBatch+1)
	}
	payload := EncodeNodeStatusRequest()
	if op, err := OperationOf(payload); err != nil || op != OpNodeStatus {
		t.Fatalf("OperationOf = %v, %v; want OpNodeStatus", op, err)
	}
	if err := DecodeNodeStatusRequest(payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := DecodeNodeStatusRequest(append(payload, 0)); err == nil {
		t.Fatal("decoded a probe with a trailing byte")
	}
	if err := DecodeNodeStatusRequest(EncodeAppliedIndexRequest()); err == nil {
		t.Fatal("decoded another op's payload as a node-status probe")
	}

	// A newer node's reply may carry fields this release does not know.
	var st NodeStatus
	body := `{"node":"narad-2","draining":true,"dispatch_backlog":7,"quarantine":{"copies":1,"bytes":10,"list":[{"kind":"partition","topic":"orders","partition":3}]},"moves":[{"topic":"orders","partition":1,"phase":"copying"}],"from_a_later_release":1}`
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	if st.Node != "narad-2" || !st.Draining || st.DispatchBacklog != 7 || st.Quarantine.Copies != 1 ||
		len(st.Quarantine.List) != 1 || st.Quarantine.List[0].Partition != 3 || len(st.Moves) != 1 || st.Moves[0].Phase != "copying" {
		t.Fatalf("decoded reply = %+v", st)
	}
}
