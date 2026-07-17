//go:build !windows

package discovery

import (
	"context"
	"net"
)

func listenUDPReuse(addr string) (net.PacketConn, error) {
	lc := net.ListenConfig{}
	return lc.ListenPacket(context.Background(), "udp4", addr)
}