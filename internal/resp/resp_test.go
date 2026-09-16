package resp

import (
	"strings"
	"testing"
)

func TestParseCommandArray(t *testing.T) {
	in := "*2\r\n$3\r\nGET\r\n$4\r\nkey\r\n"
	v, err := NewReader(strings.NewReader(in)).Read()
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != Array || len(v.Arr) != 2 {
		t.Fatalf("bad top-level: %+v", v)
	}
	if v.Arr[0].Type != BulkString || v.Arr[0].Str != "GET" {
		t.Fatalf("bad cmd: %+v", v.Arr[0])
	}
	if v.Arr[1].Str != "key" {
		t.Fatalf("bad arg: %+v", v.Arr[1])
	}
}

func TestParseNullBulk(t *testing.T) {
	v, err := NewReader(strings.NewReader("$-1\r\n")).Read()
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != BulkString || !v.Null {
		t.Fatalf("expected null bulk, got %+v", v)
	}
}

func TestParseInteger(t *testing.T) {
	v, err := NewReader(strings.NewReader(":42\r\n")).Read()
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != Integer || v.Num != 42 {
		t.Fatalf("expected integer 42, got %+v", v)
	}
}

func TestParseSimpleString(t *testing.T) {
	v, err := NewReader(strings.NewReader("+PONG\r\n")).Read()
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != SimpleString || v.Str != "PONG" {
		t.Fatalf("expected PONG, got %+v", v)
	}
}

func TestWriteRoundTrip(t *testing.T) {
	var sb strings.Builder
	if err := WriteBulk(&sb, "hello"); err != nil {
		t.Fatal(err)
	}
	if err := WriteInteger(&sb, 7); err != nil {
		t.Fatal(err)
	}
	// read back
	r := NewReader(strings.NewReader(sb.String()))
	b, _ := r.Read()
	if b.Str != "hello" {
		t.Fatalf("bulk mismatch: %q", b.Str)
	}
	i, _ := r.Read()
	if i.Num != 7 {
		t.Fatalf("int mismatch: %d", i.Num)
	}
}
