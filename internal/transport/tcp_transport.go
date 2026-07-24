package transport

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"log/slog"
	"net"

	"github.com/Lakshay309/bitgopher/internal/peer"
	"github.com/google/uuid"
)

type TCPTransport struct {
	peerID   uuid.UUID
	listener net.Listener
	tcpAddr  string

	peerManager *peer.PeerManager
}

type PacketType byte

const MaxPacketSize = 1024 * 1024 // 1MB
const (
	LengthFieldSize  = 4
	TypeFieldSize    = 1
	PacketHeaderSize = LengthFieldSize + TypeFieldSize
)

const (
	HandshakePacket PacketType = iota + 1
	PingPacket
	PongPacket
)

type Packet struct {
	Type    PacketType
	Payload []byte
}

func NewTCPTransport(pm *peer.PeerManager) (*TCPTransport, error) {
	log.Printf("TCP should listen on %s", pm.Self.TCPAddr)
	
	return &TCPTransport{
		tcpAddr:     pm.Self.TCPAddr,
		peerID:      pm.Self.ID,
		peerManager: pm,
	}, nil
}

func (t *TCPTransport) Start() error {
	listener, err := net.Listen("tcp", t.tcpAddr)
	if err != nil {
		return err
	}
	log.Printf("TCP should listen on %s", t.tcpAddr)
	
	t.listener = listener

	log.Printf("TCP listening on %s", listener.Addr())

	go t.acceptLoop()

	return nil
}

func (t *TCPTransport) Connect(addr string) error {
	if addr == "" {
		peers := t.getPeers()
		for _, p := range peers {
			if p.ID != t.peerManager.Self.ID {
				addr = p.TCPAddr
				break
			}
		}

		if addr == "" {
			return errors.New("no remote peers found")
		}
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	t.handleConn(conn)

	return nil
}

func (t *TCPTransport) acceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			slog.Error("[TCP-AcceptLoop]", "error", err)
			continue
		}

		go t.handleConn(conn)
	}
}

// TODO: understand this better and have better functionality here
func (t *TCPTransport) handleConn(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("[handleConn]", "panic", r)
			conn.Close()
		}
	}()
	peerID, err := t.performHandshake(conn)
	if err != nil {
		slog.Error("[handleConn]", "error", err)
		conn.Close()
		return
	}

	t.peerManager.PeerEventChan <- peer.PeerEvent{
		Type: peer.SetConnectionEvent,
		Command: peer.PeerCommand{
			Peer: peer.PeerInfo{
				ID:   peerID,
				Conn: conn,
			},
		},
	}

	slog.Info(
		"[handleConn]",
		"peerID", peerID,
		"remoteAddr", conn.RemoteAddr(),
	)

	// Later:
	// t.readLoop(peerID)

}

func (t *TCPTransport) performHandshake(conn net.Conn) (uuid.UUID, error) {
	// send our handshake
	err := writePacket(conn, Packet{
		Type:    HandshakePacket,
		Payload: t.peerID[:],
	})
	if err != nil {
		return uuid.Nil, err
	}

	// read there handshake
	packet, err := readPacket(conn)
	if err != nil {
		return uuid.Nil, err
	}
	if packet.Type != HandshakePacket {
		return uuid.Nil, errors.New("expected handshake packet")
	}

	if len(packet.Payload) != 16 {
		return uuid.Nil, errors.New("invalid uuid length")
	}
	peerID, err := uuid.FromBytes(packet.Payload)
	if err != nil {
		return uuid.Nil, err
	}

	return peerID, nil
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

// development
func (t *TCPTransport) ConnectionCount() int {
	resp := make(chan peer.PeerResponse)

	t.peerManager.PeerEventChan <- peer.PeerEvent{
		Type:     peer.GetConnectionCountEvent,
		Response: resp,
	}
	result := <-resp

	return result.Count
}

func (t *TCPTransport) getPeers() []peer.PeerInfo {
	resp := make(chan peer.PeerResponse)

	t.peerManager.PeerEventChan <- peer.PeerEvent{
		Type:     peer.GetPeersEvent,
		Response: resp,
	}

	result := <-resp
	return result.Peers
}
