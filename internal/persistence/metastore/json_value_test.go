package metastore

import "testing"

func TestJSONValueEqual(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{`{"type":"object"}`, "{\n  \"type\" : \"object\"\n}", true},
		{`{"a":1,"b":[true,null,"x"]}`, `{"b":[true,null,"x"],"a":1}`, true},
		{`{"n":1}`, `{"n":1.0}`, true},
		{`{"n":10}`, `{"n":1e1}`, true},
		{`{"n":10}`, `{"n":1E+1}`, true},
		{`{"n":0.5}`, `{"n":5e-1}`, true},
		{`{"n":-1.50}`, `{"n":-15e-1}`, true},
		{`{"n":0}`, `{"n":-0.0}`, true},
		{`{"n":1}`, `{"n":1.0000000000000000001}`, false},
		{`{"n":1e999999999999}`, `{"n":1e999999999998}`, false},
		{`{"n":1e999999999999}`, `{"n":1e999999999999}`, true},
		{`{"s":"\u0041"}`, `{"s":"A"}`, true},
		{`{"a":1,"a":2}`, `{"a":2}`, true},
		{`[1,2]`, `[2,1]`, false},
		{`{"a":1}`, `{"a":1,"b":2}`, false},
		{`{"a":"1"}`, `{"a":1}`, false},
		{`{"a":null}`, `{}`, false},
		{`not json`, `not json`, true},
		{`not json`, `"not json"`, false},
		{`{"a":1} {"a":1}`, `{"a":1}`, false},
	}
	for _, tc := range cases {
		if got := jsonValueEqual([]byte(tc.a), []byte(tc.b)); got != tc.want {
			t.Errorf("jsonValueEqual(%s, %s) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
		if got := jsonValueEqual([]byte(tc.b), []byte(tc.a)); got != tc.want {
			t.Errorf("jsonValueEqual(%s, %s) = %v, want %v", tc.b, tc.a, got, tc.want)
		}
	}
}
