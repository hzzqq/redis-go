package main

import (
	"flag"
	"log"

	"github.com/hzzqq/redis-go/internal/server"
)

func main() {
	addr := flag.String("addr", ":6379", "listen address, e.g. :6379")
	flag.Parse()

	s := server.New()
	log.Printf("redis-go phase-1 listening on %s (redis-cli compatible)", *addr)
	if err := s.Listen(*addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
