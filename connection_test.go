package fdfs_client

import (
	"testing"
)

func getConn(pool *ConnectionPool, t *testing.T) {
	conn, err := pool.Get()
	if err != nil {
		t.Logf("get conn error:%s", err)
	}
	if conn != nil {
		conn.Close()
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

	for i := 0; i < 1000; i++ {
		go getConn(pool, t)
	}
}
