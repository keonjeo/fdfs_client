package fdfs_client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

type TrackerClient struct {
	pool *ConnectionPool
}

func (this *TrackerClient) trackerQueryStorageStorWithoutGroup() (*StorageServer, error) {
	var (
		conn     net.Conn
		recvBuff []byte
		err      error
	)

	conn, err = this.pool.Get()
	if err != nil {
		return nil, err
	}

	th := &TrackerHeader{}
	th.cmd = TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE
	th.sendHeader(conn)

	th.recvHeader(conn)
	if th.status != 0 {
		return nil, errors.New(fmt.Sprintf("recvHeader error status :%d", th.status))
	}

	var (
		groupName      string
		ipAddr         string
		port           int64
		storePathIndex uint8
	)
	recvBuff, _, err = TcpRecvResponse(conn, th.pkgLen)
	if err != nil {
		logger.Warnf("TcpRecvResponse error :%s", err.Error())
		return nil, err
	}
	buff := bytes.NewBuffer(recvBuff)
	// #recv_fmt |-group_name(16)-ipaddr(16-1)-port(8)-store_path_index(1)|
	groupName, err = readCstr(buff, FDFS_GROUP_NAME_MAX_LEN)
	ipAddr, err = readCstr(buff, IP_ADDRESS_SIZE-1)
	binary.Read(buff, binary.BigEndian, &port)
	binary.Read(buff, binary.BigEndian, &storePathIndex)
	return &StorageServer{ipAddr, int(port), groupName, int(storePathIndex)}, nil
}

func (this *TrackerClient) trackerQueryStorageStorWithGroup(groupName string) (*StorageServer, error) {
	var (
		conn     net.Conn
		recvBuff []byte
		err      error
	)

	conn, err = this.pool.Get()
	if err != nil {
		return nil, err
	}

	th := &TrackerHeader{}
	th.cmd = TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE
	th.sendHeader(conn)

	groupBuffer := new(bytes.Buffer)
	binary.Write(groupBuffer, binary.BigEndian, groupName)
	groupBytes := groupBuffer.Bytes()

	err = TcpSendData(conn, groupBytes)
	if err != nil {
		return nil, err
	}

	th.recvHeader(conn)
	if th.status != 0 {
		return nil, errors.New(fmt.Sprintf("recvHeader error status :%d", th.status))
	}

	var (
		ipAddr         string
		port           int64
		storePathIndex uint8
	)
	recvBuff, _, err = TcpRecvResponse(conn, th.pkgLen)
	if err != nil {
		logger.Warnf("TcpRecvResponse error :%s", err.Error())
		return nil, err
	}
	buff := bytes.NewBuffer(recvBuff)
	// #recv_fmt |-group_name(16)-ipaddr(16-1)-port(8)-store_path_index(1)|
	groupName, err = readCstr(buff, FDFS_GROUP_NAME_MAX_LEN)
	ipAddr, err = readCstr(buff, IP_ADDRESS_SIZE-1)
	binary.Read(buff, binary.BigEndian, &port)
	binary.Read(buff, binary.BigEndian, &storePathIndex)
	return &StorageServer{ipAddr, int(port), groupName, int(storePathIndex)}, nil
}
