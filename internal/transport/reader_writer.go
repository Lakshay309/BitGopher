package transport

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
)

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
