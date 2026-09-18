package main

import (
	"flag"
	"log"
	"net"

	"github.com/hzzqq/redis-go/internal/server"
)

func main() {
	addr := flag.String("addr", ":6379", "listen address, e.g. :6379")
	aofPath := flag.String("aof", "", "append-only file for persistence (e.g. appendonly.aof); empty = in-memory only")
	rdbPath := flag.String("rdb", "", "RDB snapshot file (e.g. dump.rdb); loaded at startup, written by SAVE/BGSAVE")
	replicaof := flag.String("replicaof", "", "attach as replica to this master at startup (host:port)")
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
	// 先起监听再挂载复制，保证副本握手时 REPLCONF listening-port 能报到
	// 真实端口（Listen 第一行就会设置 addr）。
	go func() {
		if err := s.Listen(*addr); err != nil {
			log.Fatalf("server error: %v", err)
		}
	}()
	if *replicaof != "" {
		host, port, err := net.SplitHostPort(*replicaof)
		if err != nil {
			log.Fatalf("invalid -replicaof %q: %v", *replicaof, err)
		}
		s.BecomeReplica(host, port) // 主库未起也无所谓：复制循环会退避重连
		log.Printf("redis-go attaching as replica to %s", *replicaof)
	}
	select {} // 服务常驻（Listen 失败时上方 goroutine 会 log.Fatal 退出）
}
