package fdfs_client

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
)

var ErrClosed = errors.New("pool is closed")

type pConn struct {
	net.Conn
	pool *ConnectionPool
}

func (c pConn) Close() error {
	return c.pool.put(c.Conn)
}

type ConnectionPool struct {
	hosts []string
	port  int
	conns chan net.Conn
	mu    *sync.Mutex
}

func NewConnectionPool(hosts []string, port int, minConns int, maxConns int) (*ConnectionPool, error) {
	if minConns < 0 || maxConns <= 0 || minConns > maxConns {
		return nil, errors.New("invalid conns settings")
	}
	cp := &ConnectionPool{
		hosts: hosts,
		port:  port,
		conns: make(chan net.Conn, maxConns),
		mu:    new(sync.Mutex),
	}
	for i := 0; i < minConns; i++ {
		conn, err := cp.makeConn()
		if err != nil {
			cp.Close()
			return nil, err
		}
		cp.conns <- conn
	}
	return cp, nil
}

func (this *ConnectionPool) Get() (net.Conn, error) {
	conns := this.getConns()
	if conns == nil {
		return nil, ErrClosed
	}

	select {
	case conn := <-conns:
		if conn == nil {
			return nil, ErrClosed
		}
		return this.wrapConn(conn), nil
	default:
		conn, err := this.makeConn()
		if err != nil {
			return nil, err
		}
		return this.wrapConn(conn), nil
	}
}

func (this *ConnectionPool) Close() {
	this.mu.Lock()
	conns := this.conns
	this.conns = nil
	this.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)

	for conn := range conns {
		conn.Close()
	}
}

func (this *ConnectionPool) Len() int {
	return len(this.getConns())
}

func (this *ConnectionPool) makeConn() (net.Conn, error) {
	host := this.hosts[rand.Intn(len(this.hosts))]
	addr := fmt.Sprintf("%s:%d", host, this.port)
	return net.Dial("tcp", addr)
}

func (this *ConnectionPool) getConns() chan net.Conn {
	this.mu.Lock()
	conns := this.conns
	this.mu.Unlock()
	return conns
}

func (this *ConnectionPool) put(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.conns == nil {
		return conn.Close()
	}

	select {
	case this.conns <- conn:
		return nil
	default:
		return conn.Close()
	}
}

func (this *ConnectionPool) wrapConn(conn net.Conn) net.Conn {
	c := pConn{pool: this}
	c.Conn = conn
	return c
}

func TcpSendData(conn net.Conn, bytesStream []byte) error {
	if _, err := conn.Write(bytesStream); err != nil {
		return err
	}
	return nil
}

func TcpSendFile(conn net.Conn, filename string) error {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		return err
	}

	var fileSize int64 = 0
	if fileInfo, err := file.Stat(); err == nil {
		fileSize = fileInfo.Size()
	}

	if fileSize == 0 {
		errmsg := fmt.Sprintf("file size is zeor [%s]", filename)
		return errors.New(errmsg)
	}

	fileBuffer := make([]byte, fileSize)

	_, err = file.Read(fileBuffer)
	if err != nil {
		return err
	}

	return TcpSendData(conn, fileBuffer)
}

func TcpRecvResponse(conn net.Conn, bufferSize int64) ([]byte, int64, error) {
	recvBuff := make([]byte, bufferSize)
	var total int64
	for {
		n, err := conn.Read(recvBuff)
		total += int64(n)
		if err != nil {
			if err != io.EOF {
				return nil, 0, err
			}
			break
		}
		if total == bufferSize {
			break
		}
	}
	return recvBuff, total, nil
}

func TcpRecvFile(conn net.Conn, localFilename string, bufferSize int64) (int64, error) {
	file, err := os.Create(localFilename)
	defer file.Close()
	if err != nil {
		return 0, err
	}

	recvBuff, total, err := TcpRecvResponse(conn, bufferSize)
	if _, err := file.Write(recvBuff); err != nil {
		return 0, err
	}
	return total, nil
}
