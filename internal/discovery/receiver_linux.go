//go:build linux

package discovery

import (
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/Lakshay309/bitgopher/internal/peer"
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
	u.recvConn = conn
	defer conn.Close()

	slog.Info(
		"Receiver started",
		"iface", iface.Name,
		"group", multicastAddr,
	)

	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			select{
			case <-u.exit:
				slog.Info("exit receiver loop")
				return nil
			default:
				slog.Error("ReadFromUDP failed", "err", err)
			}
			continue
		}

		info := strings.Split(string(buffer[:n]), "|")
		if len(info) < 3 {
			continue
		}

		if u.PeerID == info[2] {
			continue
		}
		if info[0] == string(Hello) {
			peerInfo := peer.PeerInfo{
				ID:        info[2],
				TCPAddr:   info[1],
				LastSeen:  time.Now(),
				Discovery: u.discoveryMode,
			}
			u.discoveryChan <- peerInfo
		} else if info[0] == string(Bye) {
			// uuid in the remover chan that
			u.removerChan <- info[2]
		}

	}
}
