//go:build windows

package discovery

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"strings"
	"syscall"

	"golang.org/x/net/ipv4"
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

func (u *UdpServer) Receiver() error {
	buffer := make([]byte, 256)

	groupAddr, err := net.ResolveUDPAddr("udp4", multicastAddr)
	if err != nil {
		return err
	}

	iface, err := u.getInterface()
	if err != nil {
		return err
	}

	pc, err := listenUDPReuse(":9999")
	if err != nil {
		return err
	}
	defer pc.Close()

	packetConn := ipv4.NewPacketConn(pc)

	if err := packetConn.JoinGroup(iface, groupAddr); err != nil {
		return err
	}

	udpConn, ok := pc.(*net.UDPConn)
	if !ok {
		return fmt.Errorf("failed to convert PacketConn to UDPConn")
	}

	slog.Info(
		"Receiver started",
		"iface", iface.Name,
		"group", multicastAddr,
	)

	for {
		n, addr, err := udpConn.ReadFromUDP(buffer)
		if err != nil {
			slog.Error("ReadFromUDP failed", "err", err)
			continue
		}

		info := strings.Split(string(buffer[:n]), "|")
		if len(info) < 3 {
			continue
		}

		if u.PeerID == info[2] {
			continue
		}

		if _, ok := u.PeerInfo[info[2]]; !ok {
			log.Printf(
				"Received %d bytes from %s: %s",
				n,
				addr.String(),
				string(buffer[:n]),
			)
		}

		u.PeerInfo[info[2]] = info[1]
	}
}
