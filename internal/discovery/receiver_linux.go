//go:build linux

package discovery

import (
	"log/slog"
	"net"
	"time"

	"github.com/Lakshay309/bitgopher/internal/peer"
	"github.com/google/uuid"
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

		if peerID == u.peerManager.Self.ID {
			continue
		}

		switch packetType {
		case Hello:
			peerInfo := peer.PeerInfo{
				ID:        peerID,
				TCPAddr:   tcpAddr,
				LastSeen:  time.Now(),
				Discovery: u.peerManager.Self.Discovery,
			}
			peerEvent := peer.PeerEvent{
				Type: peer.DiscoveryEvent,
				Command: peer.PeerCommand{
					Peer:peerInfo,
				},
			}
			u.peerManager.PeerEventChan <- peerEvent
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
			u.peerManager.PeerEventChan <- peerEvent
		}

	}
}
