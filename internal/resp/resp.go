// Package resp implements a minimal RESP (REdis Serialization Protocol) reader/writer.
// See https://redis.io/docs/latest/develop/reference/protocol-spec/
package resp

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

// Type is the leading byte of a RESP value.
type Type byte

const (
	SimpleString Type = '+'
	Error        Type = '-'
	Integer      Type = ':'
	BulkString   Type = '$'
	Array        Type = '*'
)

// Value is a parsed RESP value. Null marks a null bulk / null array.
type Value struct {
	Type Type
	Str  string
	Num  int64
	Arr  []Value
	Null bool
}

// Reader parses RESP values from an io.Reader (typically a buffered net.Conn).
type Reader struct {
	r *bufio.Reader
}

// NewReader wraps r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

func (rd *Reader) readLine() (string, error) {
	line, err := rd.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if n := len(line); n > 0 && line[n-1] == '\n' {
		line = line[:n-1]
	}
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	return line, nil
}

func (rd *Reader) readInt() (int64, error) {
	line, err := rd.readLine()
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return 0, errors.New("resp: invalid integer: " + line)
	}
	return n, nil
}

// Read reads one RESP value from the stream.
func (rd *Reader) Read() (Value, error) {
	t, err := rd.r.ReadByte()
	if err != nil {
		return Value{}, err
	}
	return rd.readValue(Type(t))
}

func (rd *Reader) readValue(t Type) (Value, error) {
	switch t {
	case SimpleString:
		s, err := rd.readLine()
		return Value{Type: SimpleString, Str: s}, err
	case Error:
		s, err := rd.readLine()
		return Value{Type: Error, Str: s}, err
	case Integer:
		n, err := rd.readInt()
		return Value{Type: Integer, Num: n}, err
	case BulkString:
		n, err := rd.readInt()
		if err != nil {
			return Value{}, err
		}
		if n == -1 {
			return Value{Type: BulkString, Null: true}, nil
		}
		if n < -1 {
			return Value{}, errors.New("resp: invalid bulk length")
		}
		buf := make([]byte, n+2) // payload + trailing \r\n
		if _, err := io.ReadFull(rd.r, buf); err != nil {
			return Value{}, err
		}
		return Value{Type: BulkString, Str: string(buf[:n])}, nil
	case Array:
		n, err := rd.readInt()
		if err != nil {
			return Value{}, err
		}
		if n == -1 {
			return Value{Type: Array, Null: true}, nil
		}
		if n < 0 {
			return Value{}, errors.New("resp: invalid array length")
		}
		arr := make([]Value, n)
		for i := 0; i < n; i++ {
			b, err := rd.r.ReadByte()
			if err != nil {
				return Value{}, err
			}
			v, err := rd.readValue(Type(b))
			if err != nil {
				return Value{}, err
			}
			arr[i] = v
		}
		return Value{Type: Array, Arr: arr}, nil
	default:
		return Value{}, errors.New("resp: unknown type byte: " + string(t))
	}
}
