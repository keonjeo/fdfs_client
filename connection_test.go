package fdfs_client

import (
	"fmt"
	"testing"
	"time"
)

func getConn(pool *ConnectionPool) {
	conn, err := pool.Get()
	defer conn.Close()
	if err != nil {
		fmt.Printf("get conn error:%s\n", err)
	}
}

func TestGetConnection(t *testing.T) {
	hosts := []string{"10.0.1.32"}
	port := 22122
	minConns := 10
	maxConns := 150
	pool, err := NewConnectionPool(hosts, port, minConns, maxConns)
	if err != nil {
		t.Error(err)
		return
	}
	go getConn(pool)
}

func BenchmarkGetConnection(b *testing.B) {
	hosts := []string{"10.0.1.32"}
	port := 22122
	minConns := 10
	maxConns := 150
	pool, err := NewConnectionPool(hosts, port, minConns, maxConns)
	if err != nil {
		b.Error(err)
		return
	}
	b.StopTimer()
	b.StartTimer()
	for i := 0; i < 10000; i++ {
		time.Sleep(time.Microsecond)
		go getConn(pool)
	}
}
