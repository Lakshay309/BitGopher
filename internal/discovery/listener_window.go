//go:build windows

package discovery

import (
	"context"
	"net"
	"syscall"
)

func listenUDPReuse(addr string) (net.PacketConn, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var ctrlErr error

			err := c.Control(func(fd uintptr) {
				// Allow multiple processes to bind to the same UDP port.
				ctrlErr = syscall.SetsockoptInt(
					syscall.Handle(fd),
					syscall.SOL_SOCKET,
					syscall.SO_REUSEADDR,
					1,
				)
			})

			if err != nil {
				return err
			}

			return ctrlErr
		},
	}

	return lc.ListenPacket(context.Background(), "udp4", addr)
}