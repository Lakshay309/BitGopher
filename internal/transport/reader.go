package transport

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"

	"github.com/google/uuid"
)

func (t *TCPTransport) readLoop(conn net.Conn, _ uuid.UUID) {
	for {
		packet, err := readPacket(conn)
		if err != nil {
			slog.Error("[readLoop]", "err", err)
			return
		}
		switch packet.Type {
		case PingPacket:
			t.handlePing(conn)
		case PongPacket:
			t.handlePong()
		default:
			slog.Warn("unknown packet type", "type", packet.Type)
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
