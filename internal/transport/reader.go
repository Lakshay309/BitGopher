package transport

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"

	"github.com/Lakshay309/bitgopher/internal/peer"
	"github.com/google/uuid"
)

type ReadCommad struct {
	Conn   net.Conn
	PeerId uuid.UUID
	Packet Packet
}

func (t *TCPTransport) readLoop(conn net.Conn, remotePeerID uuid.UUID) {
	for {
		packet, err := readPacket(conn)
		if err != nil {
			t.peerManager.PeerEventChan <- peer.PeerEvent{
				Type: peer.RemoveConnectionEvent,
				Command: peer.PeerCommand{
					Peer: peer.PeerInfo{
						ID: remotePeerID,
						Conn: conn,
					},
				},
			}
			slog.Error("[readLoop]", "err", err)
			return
		}
		// upadate in run function in peermanager
		// used command as we are not asking for any thing from the peerManger we have to update thing in the peerManager map
		t.peerManager.PeerEventChan <- peer.PeerEvent{
			Type: peer.SetLastActivity,
			Command: peer.PeerCommand{
				Peer: peer.PeerInfo{
					ID: remotePeerID,
				},
			},
		}

		// controller will read from the read chan
		t.ReadChan <- ReadCommad{
			Conn:   conn,
			PeerId: remotePeerID,
			Packet: packet,
		}
	}
}

func readPacket(conn net.Conn) (Packet, error) {
	// read first 4 byte to get length fiels
	lengthBuf := make([]byte, LengthFieldSize)
	if _, err := io.ReadFull(conn, lengthBuf); err != nil {
		return Packet{}, err
	}
	packetLength := binary.BigEndian.Uint32(lengthBuf)

	// checking for invalid packet
	if packetLength < TypeFieldSize {
		return Packet{}, errors.New("invalid packet length")
	}
	if packetLength > MaxPacketSize {
		return Packet{}, errors.New("packet too large")
	}

	// read the remaining bytes (Type+Payload)
	packetBuf := make([]byte, packetLength)
	if _, err := io.ReadFull(conn, packetBuf); err != nil {
		return Packet{}, err
	}
	packet := Packet{
		Type:    PacketType(packetBuf[0]),
		Payload: packetBuf[1:],
	}

	return packet, nil
}
