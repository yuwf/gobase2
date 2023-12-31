package tcp

// https://github.com/yuwf/gobase2

import (
	"net"
	"time"
)

func TcpPortCheck(addr string, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}
