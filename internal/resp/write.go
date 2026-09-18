package resp

import (
	"io"
	"strconv"
)

// WriteSimpleString writes a RESP simple string (+...\r\n).
func WriteSimpleString(w io.Writer, s string) error {
	_, err := io.WriteString(w, "+"+s+"\r\n")
	return err
}

// WriteError writes a RESP error (-...\r\n).
func WriteError(w io.Writer, s string) error {
	_, err := io.WriteString(w, "-"+s+"\r\n")
	return err
}

// WriteInteger writes a RESP integer (:N\r\n).
func WriteInteger(w io.Writer, n int64) error {
	_, err := io.WriteString(w, ":"+strconv.FormatInt(n, 10)+"\r\n")
	return err
}

// WriteBulk writes a RESP bulk string ($len\r\n...\r\n).
func WriteBulk(w io.Writer, s string) error {
	if _, err := io.WriteString(w, "$"+strconv.Itoa(len(s))+"\r\n"); err != nil {
		return err
	}
	if _, err := io.WriteString(w, s+"\r\n"); err != nil {
		return err
	}
	return nil
}

// WriteNullBulk writes a RESP null bulk ($-1\r\n).
func WriteNullBulk(w io.Writer) error {
	_, err := io.WriteString(w, "$-1\r\n")
	return err
}

// WriteArrayHeader writes an array header of n elements (*n\r\n).
func WriteArrayHeader(w io.Writer, n int) error {
	_, err := io.WriteString(w, "*"+strconv.Itoa(n)+"\r\n")
	return err
}

// WriteValue writes any Value recursively (arrays of scalars supported).
func WriteValue(w io.Writer, v Value) error {
	switch v.Type {
	case SimpleString:
		return WriteSimpleString(w, v.Str)
	case Error:
		return WriteError(w, v.Str)
	case Integer:
		return WriteInteger(w, v.Num)
	case BulkString:
		if v.Null {
			return WriteNullBulk(w)
		}
		return WriteBulk(w, v.Str)
	case Array:
		if v.Null {
			// null array（*-1）：EXEC 中止等场景，与 null bulk（$-1）区分
			return WriteArrayHeader(w, -1)
		}
		if err := WriteArrayHeader(w, len(v.Arr)); err != nil {
			return err
		}
		for _, item := range v.Arr {
			if err := WriteValue(w, item); err != nil {
				return err
			}
		}
		return nil
	default:
		return WriteError(w, "ERR internal: unsupported reply type")
	}
}
