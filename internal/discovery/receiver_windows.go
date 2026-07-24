//go:build windows

package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"syscall"
	"time"

	"github.com/Lakshay309/bitgopher/internal/peer"
	"github.com/google/uuid"
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
	u.recvConn = udpConn

	slog.Info(
		"Receiver started",
		"iface", iface.Name,
		"group", multicastAddr,
	)

	for {
		n, _, err := udpConn.ReadFromUDP(buffer)
		if err != nil {
			select {
			case <-u.exit:
				slog.Info("exit receiver loop")
				return nil
			default:
				slog.Error("ReadFromUDP failed", "err", err)
			}
			continue
		}

		// decrypt the data first
		PlainData, err := u.decryptData(buffer[:n])
		if err != nil {
			// remove this as it can become annoying
			slog.Error(err.Error())
		}

		if len(PlainData) < 2 {
			slog.Error("invalid packet")
			continue
		}

		packetType := PacketType(PlainData[0])
		addrLen := int(PlainData[1])

		if len(PlainData) != 2+addrLen+UUIDSize {
			slog.Error("invalid packet length")
			continue
		}
	

		tcpAddr := string(PlainData[2 : 2+addrLen])

		var peerID uuid.UUID

		copy(peerID[:], PlainData[2+addrLen:2+addrLen+UUIDSize])

		if peerID == u.PeerID {
			continue
		}

		switch packetType {
		case Hello:
			peerInfo := peer.PeerInfo{
				ID:        peerID,
				TCPAddr:   tcpAddr,
				LastSeen:  time.Now(),
				Discovery: u.discoveryMode,
			}
			peerEvent := peer.PeerEvent{
				Type: peer.DiscoveryEvent,
				Command: peer.PeerCommand{
					Peer:peerInfo,
				},
			}
			u.eventChan <- peerEvent
		case Bye:
			// uuid in the remover chan that
			peerEvent := peer.PeerEvent{
				Type:peer.RemovePeerEvent,
				Command: peer.PeerCommand{
					Peer: peer.PeerInfo{
						ID: peerID,
					},
				},
			}
			u.eventChan <- peerEvent
		}
	}
}
