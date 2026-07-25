package transport

import (
	"encoding/binary"
	"log/slog"
	"net"

	"github.com/Lakshay309/bitgopher/internal/peer"
	"github.com/google/uuid"
)

type WriteCommand struct {
	Conn   net.Conn
	PeerID uuid.UUID
	Packet Packet
}

func (t *TCPTransport) writeLoop() {
	for cmd := range t.WriteChan {
		slog.Info("[writeLoop]", "packet", cmd.Packet.Type)
		err := writePacket(cmd.Conn, cmd.Packet)
        if err != nil {
            slog.Error("[writeLoop]", "err", err)
			continue
        }
		t.peerManager.PeerEventChan<-peer.PeerEvent{
			Type: peer.SetLastActivity,
			Command: peer.PeerCommand{
				Peer: peer.PeerInfo{
					ID: cmd.PeerID,
				},
			},
		}
	}
}

func writePacket(conn net.Conn, packet Packet) error {
	// length doesnot include the 4-byte length field itself
	packetLength := uint32(TypeFieldSize + len(packet.Payload))

	finalPacket := make([]byte, 0, PacketHeaderSize+len(packet.Payload))

	// encoding the length in 4 byte big Endian
	lengthBuf := make([]byte, LengthFieldSize)
	binary.BigEndian.PutUint32(lengthBuf, packetLength)

	// building the packet
	finalPacket = append(finalPacket, lengthBuf...)
	finalPacket = append(finalPacket, byte(packet.Type))
	finalPacket = append(finalPacket, packet.Payload...)

	// Write everything.
	n, err := conn.Write(finalPacket)
	if err != nil {
		slog.Error(
			"[writePacket]",
			"error", err,
			"bytesWritten", n,
		)
		return err
	}

	slog.Info(
		"[writePacket]",
		"bytesWritten", n,
	)

	return nil

}
