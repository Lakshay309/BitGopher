package transport

import (
	"errors"
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
	// TODO: better will be getting a random port from the os and then sending that port to the udp server and peerManager  also start the tcp server first after that we can start the udp server also the peer manager is the first thing that will run
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

	// after successfull handshake
	go t.readLoop(conn, peerID)

}

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



// send ping

func (t *TCPTransport) SendPing(conn net.Conn) error {
	return writePacket(conn, Packet{
		Type: PingPacket,
	})
}