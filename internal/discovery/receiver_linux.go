//go:build linux

package discovery

import (
	"log"
	"log/slog"
	"net"
	"strings"
)

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

	conn, err := net.ListenMulticastUDP("udp4", iface, groupAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	slog.Info(
		"Receiver started",
		"iface", iface.Name,
		"group", multicastAddr,
	)

	for {
		n, addr, err := conn.ReadFromUDP(buffer)
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