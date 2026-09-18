package main

import (
	"flag"
	"log"

	"github.com/hzzqq/redis-go/internal/server"
)

func main() {
	addr := flag.String("addr", ":6379", "listen address, e.g. :6379")
	aofPath := flag.String("aof", "", "append-only file for persistence (e.g. appendonly.aof); empty = in-memory only")
	rdbPath := flag.String("rdb", "", "RDB snapshot file (e.g. dump.rdb); loaded at startup, written by SAVE/BGSAVE")
	flag.Parse()

	var s *server.Server
	var err error
	if *aofPath != "" || *rdbPath != "" {
		// 加载顺序对齐 Redis：先 RDB 快照，再回放 AOF（两者都开时 AOF 状态优先）
		s, err = server.NewWithPersist(*rdbPath, *aofPath)
		if err != nil {
			log.Fatalf("server error: %v", err)
		}
		defer s.Close()
		log.Printf("redis-go listening on %s (aof: %q, rdb: %q)", *addr, *aofPath, *rdbPath)
	} else {
		s = server.New()
		log.Printf("redis-go listening on %s (in-memory, no persistence)", *addr)
	}
	if err := s.Listen(*addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
