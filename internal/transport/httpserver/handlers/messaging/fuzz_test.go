package messaging

// Fuzz targets for the hand-rolled query walkers on the data-plane hot
// path. consumeQueryFromRawQuery documents exact equivalence with
// url.ParseQuery for its keys, so it is fuzzed differentially against
// it; the produce and ack walkers reject some inputs ParseQuery drops,
// so they are compared only where both accept the query.

import (
	"net/url"
	"strconv"
	"strings"
	"testing"
)

var querySeeds = []string{
	"",
	"partition=1&offset=2&wait=5s&local_only=1",
	"partition=1&partition=2",
	"partition=%31&offset=%ZZ&wait=1s",
	"partition=1;offset=2",
	"key=k1&partition=3",
	"key=%2Bplus&partition=0",
	"key=a&key=b",
	"receipt_handle=0:1:2&extend=true",
	"receipt_handle=0%3A1%3A2&extend=0",
	"receipt_handle=&extend=nope",
	"%72eceipt_handle=0:1:2",
	"a=b&&=&c",
	strings.Repeat("wait=1s&", 50),
	"wait=%",
	"partition=-1&offset=-1",
}

// FuzzConsumeQuery checks that the consume walker returns exactly what
// url.ParseQuery followed by url.Values.Get returns for each key.
func FuzzConsumeQuery(f *testing.F) {
	for _, s := range querySeeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		got := consumeQueryFromRawQuery(raw)
		values, _ := url.ParseQuery(raw)
		want := consumeQuery{
			partition: values.Get("partition"),
			offset:    values.Get("offset"),
			wait:      values.Get("wait"),
			localOnly: values.Get("local_only"),
		}
		if got != want {
			t.Fatalf("consumeQueryFromRawQuery(%q)\n got %+v\nwant %+v", raw, got, want)
		}
	})
}

// FuzzProduceQuery checks the produce walker never panics and, whenever
// it and url.ParseQuery both accept a query with no repeated key, agrees
// with ParseQuery on key and partition.
func FuzzProduceQuery(f *testing.F) {
	for _, s := range querySeeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		got, err := produceQueryFromRawQuery(raw)
		values, perr := url.ParseQuery(raw)
		if err != nil || perr != nil {
			return
		}
		if len(values["key"]) > 1 || len(values["partition"]) > 1 {
			t.Fatalf("produceQueryFromRawQuery(%q) accepted a duplicate parameter", raw)
		}
		if got.key != values.Get("key") {
			t.Fatalf("produceQueryFromRawQuery(%q).key = %q, want %q", raw, got.key, values.Get("key"))
		}
		if p := values.Get("partition"); p != "" {
			n, aerr := strconv.Atoi(p)
			if aerr != nil || n < 0 {
				t.Fatalf("produceQueryFromRawQuery(%q) accepted partition %q", raw, p)
			}
			if !got.hasPartition || got.partition != n {
				t.Fatalf("produceQueryFromRawQuery(%q).partition = (%v, %d), want %d", raw, got.hasPartition, got.partition, n)
			}
		} else if got.hasPartition {
			t.Fatalf("produceQueryFromRawQuery(%q) found a partition ParseQuery did not", raw)
		}
	})
}

// FuzzAckParams checks the ack walker never panics and agrees with
// url.ParseQuery on the receipt handle and the extend mode whenever both
// accept the query.
func FuzzAckParams(f *testing.F) {
	for _, s := range querySeeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		handle, found, mode, err := ackParamsFromRawQuery(raw)
		values, perr := url.ParseQuery(raw)
		if err != nil || perr != nil {
			return
		}
		_, wantFound := values["receipt_handle"]
		if found != wantFound {
			t.Fatalf("ackParamsFromRawQuery(%q) found=%v, want %v", raw, found, wantFound)
		}
		if handle != values.Get("receipt_handle") {
			t.Fatalf("ackParamsFromRawQuery(%q) handle = %q, want %q", raw, handle, values.Get("receipt_handle"))
		}
		var wantMode ackMode
		switch v := values.Get("extend"); v {
		case "", "false":
			wantMode = ackCommit
		case "true", "1":
			wantMode = ackExtend
		case "0":
			wantMode = ackNack
		default:
			wantMode = ackModeInvalid
		}
		if mode != wantMode {
			t.Fatalf("ackParamsFromRawQuery(%q) mode = %d, want %d", raw, mode, wantMode)
		}
	})
}
