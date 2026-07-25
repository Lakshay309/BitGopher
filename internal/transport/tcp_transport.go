package transport

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"time"

	"github.com/Lakshay309/bitgopher/internal/peer"
	"github.com/google/uuid"
)

type TCPTransport struct {
	listener net.Listener
	// controller will put the packet in the write chan
	WriteChan chan WriteCommand
	// controller will read from the read chan
	ReadChan chan ReadCommad

	peerManager *peer.PeerManager
}

type PacketType byte

const MaxPacketSize = 1024 * 1024 // 1MB
const (
	LengthFieldSize  = 4
	TypeFieldSize    = 1
	PacketHeaderSize = LengthFieldSize + TypeFieldSize
	WriteChanSize    = 64
	ReadChanSize     = 64
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
		peerManager: pm,
		WriteChan:   make(chan WriteCommand, WriteChanSize),
		ReadChan:    make(chan ReadCommad, ReadChanSize),
	}, nil
}

func (t *TCPTransport) Start() error {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return err
	}
	tcpAddr := listener.Addr().(*net.TCPAddr)
	t.peerManager.SetTCPAddr(fmt.Sprintf(":%d", tcpAddr.Port))
	

	t.listener = listener

	log.Printf("TCP listening on %s", listener.Addr())
	log.Printf("working good: %s",t.peerManager.Self.TCPAddr)

	go t.acceptLoop()

	go t.writeLoop()

	return nil
}

func (t *TCPTransport) Connect(addr string) error {
	const (
		maxAttempts = 3
		retryDelay  = 500 * time.Millisecond
	)

	var err error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var conn net.Conn
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			t.handleConn(conn)
			return nil
		}

		if attempt < maxAttempts {
			log.Printf("Connect attempt %d/%d to %s failed: %v. Retrying...",
				attempt, maxAttempts, addr, err)
			time.Sleep(retryDelay)
		}
	}

	return fmt.Errorf("failed to connect to %s after %d attempts: %w",
		addr, maxAttempts, err)
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

	// after successfull handshake
	go t.readLoop(conn, peerID)

}

func (t *TCPTransport) performHandshake(conn net.Conn) (uuid.UUID, error) {
	// send our handshake
	err := writePacket(conn, Packet{
		Type:    HandshakePacket,
		Payload: t.peerManager.Self.ID[:],
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
