// Package persist, RDB file: a binary snapshot of the store.
//
// File layout (all integers little-endian, CRC64/ECMA-182 over the payload
// between the length header and the trailer):
//
//	magic "REDIS0059"            9 bytes (signature + version)
//	uint64 payloadLen            8 bytes (length of the payload section)
//	payload payloadLen bytes     the serialized keys
//	uint64 crc                   8 bytes (CRC64/ECMA-182 of payloadLen || payload)
//
// Payload is a sequence of records, one per key:
//	uint8 kind          0=string 1=list 2=hash 3=set 4=zset
//	uint64 keyLen + key
//	uint64 ttlMs        0 = no TTL, else absolute ms epoch
//	kind-specific body:
//	  string: uint64 valLen + val
//	  list:   uint32 count + count*(uint64 len + bytes)
//	  hash:   uint32 count + count*(uint64 fLen + field, uint64 vLen + val)
//	  set:    uint32 count + count*(uint64 mLen + member)
//	  zset:   uint32 count + count*(float64 score + uint64 mLen + member)
//
// Load validates the CRC, returns an error on mismatch or truncation.
package persist

import (
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"math"
	"os"
	"time"

	"github.com/hzzqq/redis-go/internal/store"
)

var crcTable = crc64.MakeTable(crc64.ECMA)

// Magic header: "REDIS" + 4-char version. Bump when the on-disk shape changes.
const rdbMagic = "REDIS0059"

// SaveRDB writes a binary snapshot of s.Export() to path atomically (tmp +
// rename, mirroring AOF.Rewrite). Existing files are overwritten.
func SaveRDB(path string, keys []store.Exported) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("rdb: save: open %s: %w", tmp, err)
	}
	werr := writeRDB(f, keys)
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		os.Remove(tmp)
		return fmt.Errorf("rdb: save: %w", werr)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rdb: save: rename: %w", err)
	}
	return nil
}

func writeRDB(w io.Writer, keys []store.Exported) error {
	// 先把 payload 序列化到内存，以便算 CRC 再一次写出。
	var payload []byte
	for _, e := range keys {
		var err error
		payload, err = appendRecord(payload, e)
		if err != nil {
			return err
		}
	}
	if _, err := w.Write([]byte(rdbMagic)); err != nil {
		return err
	}
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(payload)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	// CRC 覆盖 lenBuf + payload（先写 lenBuf 再写 payload，所以按写出顺序校验）
	crc := crc64.New(crcTable)
	crc.Write(lenBuf[:])
	crc.Write(payload)
	binary.LittleEndian.PutUint64(lenBuf[:], crc.Sum64())
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	return nil
}

func appendRecord(buf []byte, e store.Exported) ([]byte, error) {
	var kind byte
	switch e.Kind {
	case "string":
		kind = 0
	case "list":
		kind = 1
	case "hash":
		kind = 2
	case "set":
		kind = 3
	case "zset":
		kind = 4
	default:
		return nil, fmt.Errorf("rdb: unknown kind %q", e.Kind)
	}
	buf = append(buf, kind)
	buf = appendString(buf, e.Key)
	var ttlMs uint64
	if !e.Expiry.IsZero() {
		ttlMs = uint64(e.Expiry.UnixMilli())
	}
	buf = appendUint64(buf, ttlMs)
	switch e.Kind {
	case "string":
		buf = appendString(buf, e.Str)
	case "list":
		buf = appendUint32(buf, uint32(len(e.List)))
		for _, s := range e.List {
			buf = appendString(buf, s)
		}
	case "hash":
		buf = appendUint32(buf, uint32(len(e.Hash)))
		for _, p := range e.Hash {
			buf = appendString(buf, p[0])
			buf = appendString(buf, p[1])
		}
	case "set":
		buf = appendUint32(buf, uint32(len(e.Set)))
		for _, m := range e.Set {
			buf = appendString(buf, m)
		}
	case "zset":
		buf = appendUint32(buf, uint32(len(e.ZItems)))
		for _, it := range e.ZItems {
			var fb [8]byte
			binary.LittleEndian.PutUint64(fb[:], math.Float64bits(it.Score))
			buf = append(buf, fb[:]...)
			buf = appendString(buf, it.Member)
		}
	}
	return buf, nil
}

func appendString(buf []byte, s string) []byte {
	buf = appendUint64(buf, uint64(len(s)))
	return append(buf, s...)
}

func appendUint64(buf []byte, v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return append(buf, b[:]...)
}

func appendUint32(buf []byte, v uint32) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	return append(buf, b[:]...)
}

// LoadRDB reads and validates the RDB file at path. A missing file returns
// (nil, nil) so a fresh server starts clean. CRC mismatch / truncation is an
// error. The returned slice is ready for store.Replay.
func LoadRDB(path string) ([]store.Exported, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("rdb: load %s: %w", path, err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("rdb: load %s: %w", path, err)
	}
	return parseRDB(data)
}

func parseRDB(data []byte) ([]store.Exported, error) {
	if len(data) < len(rdbMagic)+16 {
		return nil, fmt.Errorf("rdb: file too short (%d bytes)", len(data))
	}
	if string(data[:len(rdbMagic)]) != rdbMagic {
		return nil, fmt.Errorf("rdb: bad magic %q", data[:len(rdbMagic)])
	}
	off := len(rdbMagic)
	payloadLen := binary.LittleEndian.Uint64(data[off : off+8])
	off += 8
	if uint64(len(data)-off-8) < payloadLen {
		return nil, fmt.Errorf("rdb: truncated payload: header says %d, have %d", payloadLen, uint64(len(data)-off-8))
	}
	payload := data[off : off+int(payloadLen)]
	off += int(payloadLen)
	storedCRC := binary.LittleEndian.Uint64(data[off : off+8])
	crc := crc64.New(crcTable)
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], payloadLen)
	crc.Write(lenBuf[:])
	crc.Write(payload)
	if crc.Sum64() != storedCRC {
		return nil, fmt.Errorf("rdb: CRC mismatch (file %x, computed %x)", storedCRC, crc.Sum64())
	}
	return decodeRecords(payload)
}

func decodeRecords(buf []byte) ([]store.Exported, error) {
	var out []store.Exported
	for len(buf) > 0 {
		rec, rest, err := decodeRecord(buf)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
		buf = rest
	}
	return out, nil
}

func decodeRecord(buf []byte) (store.Exported, []byte, error) {
	if len(buf) < 1 {
		return store.Exported{}, nil, fmt.Errorf("rdb: truncated record header")
	}
	kind := buf[0]
	buf = buf[1:]
	var e store.Exported
	switch kind {
	case 0:
		e.Kind = "string"
	case 1:
		e.Kind = "list"
	case 2:
		e.Kind = "hash"
	case 3:
		e.Kind = "set"
	case 4:
		e.Kind = "zset"
	default:
		return store.Exported{}, nil, fmt.Errorf("rdb: unknown kind byte %d", kind)
	}
	var err error
	e.Key, buf, err = readString(buf)
	if err != nil {
		return store.Exported{}, nil, err
	}
	ttlMs, buf, err := readUint64(buf)
	if err != nil {
		return store.Exported{}, nil, err
	}
	if ttlMs > 0 {
		e.Expiry = time.UnixMilli(int64(ttlMs))
	}
	switch e.Kind {
	case "string":
		e.Str, buf, err = readString(buf)
	case "list":
		var n uint32
		n, buf, err = readUint32(buf)
		if err != nil {
			return store.Exported{}, nil, err
		}
		e.List = make([]string, 0, n)
		for i := uint32(0); i < n; i++ {
			var s string
			s, buf, err = readString(buf)
			if err != nil {
				return store.Exported{}, nil, err
			}
			e.List = append(e.List, s)
		}
	case "hash":
		var n uint32
		n, buf, err = readUint32(buf)
		if err != nil {
			return store.Exported{}, nil, err
		}
		e.Hash = make([][2]string, 0, n)
		for i := uint32(0); i < n; i++ {
			var f, v string
			f, buf, err = readString(buf)
			if err != nil {
				return store.Exported{}, nil, err
			}
			v, buf, err = readString(buf)
			if err != nil {
				return store.Exported{}, nil, err
			}
			e.Hash = append(e.Hash, [2]string{f, v})
		}
	case "set":
		var n uint32
		n, buf, err = readUint32(buf)
		if err != nil {
			return store.Exported{}, nil, err
		}
		e.Set = make([]string, 0, n)
		for i := uint32(0); i < n; i++ {
			var s string
			s, buf, err = readString(buf)
			if err != nil {
				return store.Exported{}, nil, err
			}
			e.Set = append(e.Set, s)
		}
	case "zset":
		var n uint32
		n, buf, err = readUint32(buf)
		if err != nil {
			return store.Exported{}, nil, err
		}
		e.ZItems = make([]store.ZItem, 0, n)
		for i := uint32(0); i < n; i++ {
			if len(buf) < 8 {
				return store.Exported{}, nil, fmt.Errorf("rdb: truncated zset score")
			}
			score := math.Float64frombits(binary.LittleEndian.Uint64(buf[:8]))
			buf = buf[8:]
			var m string
			m, buf, err = readString(buf)
			if err != nil {
				return store.Exported{}, nil, err
			}
			e.ZItems = append(e.ZItems, store.ZItem{Member: m, Score: score})
		}
	}
	if err != nil {
		return store.Exported{}, nil, err
	}
	return e, buf, nil
}

func readString(buf []byte) (string, []byte, error) {
	n, rest, err := readUint64(buf)
	if err != nil {
		return "", nil, err
	}
	if uint64(len(rest)) < n {
		return "", nil, fmt.Errorf("rdb: truncated string (need %d, have %d)", n, len(rest))
	}
	return string(rest[:n]), rest[n:], nil
}

func readUint64(buf []byte) (uint64, []byte, error) {
	if len(buf) < 8 {
		return 0, nil, fmt.Errorf("rdb: truncated uint64")
	}
	return binary.LittleEndian.Uint64(buf[:8]), buf[8:], nil
}

func readUint32(buf []byte) (uint32, []byte, error) {
	if len(buf) < 4 {
		return 0, nil, fmt.Errorf("rdb: truncated uint32")
	}
	return binary.LittleEndian.Uint32(buf[:4]), buf[4:], nil
}
