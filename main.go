package main

import (
	"flag"
	"log"

	"github.com/hzzqq/redis-go/internal/server"
)

func main() {
	addr := flag.String("addr", ":6379", "listen address, e.g. :6379")
	aofPath := flag.String("aof", "", "append-only file for persistence (e.g. appendonly.aof); empty = in-memory only")
	flag.Parse()

	var s *server.Server
	var err error
	if *aofPath != "" {
		// 回放既有 AOF（容忍截断尾部）后继续追加写命令
		s, err = server.NewWithAOF(*aofPath)
		if err != nil {
			log.Fatalf("server error: %v", err)
		}
		defer s.Close()
		log.Printf("redis-go listening on %s (aof: %s)", *addr, *aofPath)
	} else {
		s = server.New()
		log.Printf("redis-go listening on %s (in-memory, no persistence)", *addr)
	}
	if err := s.Listen(*addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
