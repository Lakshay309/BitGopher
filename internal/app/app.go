package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/Lakshay309/bitgopher/internal/discovery"
	"github.com/Lakshay309/bitgopher/internal/fileManager"
	"github.com/Lakshay309/bitgopher/internal/peer"
	"github.com/Lakshay309/bitgopher/internal/transport"
	"github.com/google/uuid"
)

type UILog struct {
	Payload   string
	Error     error
	Originate string
}

const (
	UILogChanSize = 32
	UIChanSize    = 32
)

type App struct {
	transport   *transport.TCPTransport
	peerManager *peer.PeerManager
	discovery   *discovery.UdpServer
	fileManager *fileManager.FileManager
	exit        chan struct{}
	UiLogChan   chan UILog
	UiChan      chan UICommand
}

func NewApp(mode common.DiscoveryMode, password string) (*App, error) {
	peerManager := peer.NewPeerManager(mode)
	transport, err := transport.NewTCPTransport(peerManager)
	if err != nil {
		return nil, err
	}
	udpserver, err := discovery.NewUdpServer(peerManager, password)
	if err != nil {
		return nil, err
	}

	// initiating the fileManager
	dir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	sharedDir := filepath.Join(dir, ".share")

	fileManager, err := fileManager.NewFileManager(sharedDir, peerManager.Self.ID)
	if err != nil {
		return nil, err
	}

	return &App{
		transport:   transport,
		discovery:   udpserver,
		peerManager: peerManager,
		fileManager: fileManager,
		exit:        make(chan struct{}),
		UiLogChan:   make(chan UILog, UILogChanSize),
		UiChan:      make(chan UICommand, UIChanSize),
	}, nil
}

func (a *App) Start() error {
	// Start file manager. It starts its own goroutine internally.
	a.fileManager.Run()

	// start tcp server
	if err := a.transport.Start(); err != nil {
		a.UiLogChan <- UILog{
			Payload:   "Failed to start TCP transport.",
			Error:     err,
			Originate: "App.Start",
		}
		return err
	}

	// run peer manager
	go a.peerManager.Run()

	// run the coordination system
	go a.Run()

	// start discovery server
	if err := a.discovery.Start(); err != nil {
		a.UiLogChan <- UILog{
			Payload:   "Failed to start discovery server.",
			Error:     err,
			Originate: "App.Start",
		}
		return err
	}

	return nil
}

// Main function for the app reads the readchan in the transport and have business logic for readchan
func (a *App) Run() {
	for {
		select {
		case cmd, ok := <-a.transport.ReadChan:
			if !ok {
				return
			}
			a.handlePacket(cmd)

		case cmd := <-a.UiChan:
			a.handleEvent(cmd)

		case <-a.exit:
			return

		}
	}
}

func (a *App) handleEvent(cmd UICommand) {
	switch cmd.Type {
	case UIPing:
		a.handleUIPingEvent(cmd)

	case UIPeers:
		a.handleUIPeersEvent(cmd.Response)

	case UIDisconnect:
		a.handleUIDisconnect(cmd)

	case UIBlackList:
		a.handleBlacklist(cmd)

	case UIGetBlackList:
		a.handleGetBlacklist(cmd)

	// we have to send response chan in these functions
	case UISearchForAFile:
		a.handleSearchForAFile(cmd)
	case UIGetSeededFiles:
		a.handleGetSeededFiles(cmd)
	case UIGetSeededFileUsingHash:
		a.handleGetSeededFileUsingHash(cmd)
	case UISeedLocalFile:
		a.handleSeedLocalFile(cmd)

	}
}

func (a *App) handlePacket(cmd transport.ReadCommad) {
	switch cmd.Packet.Type {
	case transport.PingPacket:
		a.handlePing(cmd)
	case transport.PongPacket:
		a.handlePong(cmd)
	default:
		slog.Warn("unknown packet", "type", cmd.Packet.Type)
	}
}

func (a *App) handleGetBlacklist(cmd UICommand) {
	ctx, cancel := context.WithTimeout(context.Background(), common.ContextTimeInMinute*time.Minute)
	defer cancel()

	resp := make(chan peer.PeerResponse)
	a.peerManager.PeerEventChan <- peer.PeerEvent{
		Type:     peer.GetBlackListPeer,
		Response: resp,
	}
	select {
	case <-ctx.Done():
		slog.Error("[handleGetBlacklist] Time OUT")
		cmd.Response <- UIResponse{
			Err: fmt.Errorf("TIme out error"),
		}
	case result := <-resp:
		peers := result.Peers
		cmd.Response <- UIResponse{
			Payload: peers,
		}
	}
}

func (a *App) handleBlacklist(cmd UICommand) {
	a.peerManager.PeerEventChan <- peer.PeerEvent{
		Type: peer.HandleBlackListPeer,
		Command: peer.PeerCommand{
			Peer: peer.PeerInfo{
				ID: cmd.RemotePeerID,
			},
		},
	}
}

//* helper function

func (a *App) GetPeers() []peer.PeerInfo {
	resp := make(chan peer.PeerResponse)
	a.peerManager.PeerEventChan <- peer.PeerEvent{
		Type:     peer.GetPeersEvent,
		Response: resp,
	}
	result := <-resp
	peers := result.Peers
	return peers
}

func (a *App) GetPeer(ID uuid.UUID) *peer.PeerInfo {
	resp := make(chan peer.PeerResponse)
	a.peerManager.PeerEventChan <- peer.PeerEvent{
		Type: peer.GetPeerEvent,
		Query: peer.PeerQuery{
			PeerID: ID,
		},
		Response: resp,
	}
	result := <-resp
	peer := result.Peer
	return peer
}

func (a *App) DialSomeone() {
	peers := a.GetPeers()
	for _, peer := range peers {
		if peer.ID != a.peerManager.Self.ID {
			if err := a.transport.Connect(peer.TCPAddr); err != nil {
				slog.Error("[dailsomeone]", "err", err)
			}
		}
	}
}

func (a *App) DialPeer(tcpAddr string, remotePeerID uuid.UUID) error {
	peer := a.GetPeer(remotePeerID)
	if peer != nil && peer.Conn != nil {
		return nil
	}

	return a.transport.Connect(tcpAddr)
}

func (a *App) DisconnectPeer(peerID uuid.UUID) {
	a.peerManager.PeerEventChan <- peer.PeerEvent{
		Type: peer.RemovePeerEvent,
		Command: peer.PeerCommand{
			Peer: peer.PeerInfo{
				ID: peerID,
			},
		},
	}
}

func (a *App) SendPacketSync(conn net.Conn, peerID uuid.UUID, packetType transport.PacketType, payload []byte, wantResult bool) error {
	if peerID == uuid.Nil {
		return errors.New("invalid peer ID")
	}

	if conn == nil {
		peer := a.GetPeer(peerID)
		if peer == nil {
			return errors.New("peer not found")
		}
		if peer.Conn == nil {
			return errors.New("peer is not connected")
		}
		conn = peer.Conn
	}

	if conn == nil {
		return errors.New("peer has no connection")
	}
	var resp chan error
	if wantResult {
		resp = make(chan error, 1)
	} else {
		resp = nil
	}
	a.transport.WriteChan <- transport.WriteCommand{
		Conn:   conn,
		PeerID: peerID,
		Packet: transport.Packet{
			Type:    packetType,
			Payload: payload,
		},
		Response: resp,
	}
	if wantResult {
		select {
		case err := <-resp:
			return err

		case <-time.After(30 * time.Second):
			return errors.New("write response timed out")
		}
	}

	return nil
}

func (a *App) SendPacket(conn net.Conn, peerID uuid.UUID, packetType transport.PacketType, payload []byte) error {
	if peerID == uuid.Nil {
		return errors.New("invalid peer ID")
	}

	if conn == nil {
		peer := a.GetPeer(peerID)
		if peer == nil {
			return errors.New("peer not found")
		}
		if peer.Conn == nil {
			return errors.New("peer is not connected")
		}
		conn = peer.Conn
	}

	if conn == nil {
		return errors.New("peer has no connection")
	}

	a.transport.WriteChan <- transport.WriteCommand{
		Conn:   conn,
		PeerID: peerID,
		Packet: transport.Packet{
			Type:    packetType,
			Payload: payload,
		},
	}
	return nil
}

//* handle packet function from readchan

func (a *App) handlePing(cmd transport.ReadCommad) {
	if err := a.SendPacket(cmd.Conn, cmd.PeerId, transport.PongPacket, nil); err != nil {
		slog.Error("failed to send pong", "err", err)
	}
}

func (a *App) handlePong(cmd transport.ReadCommad) {
	a.UiLogChan <- UILog{
		Payload:   fmt.Sprintf("PONG from %s", cmd.PeerId.String()),
		Error:     nil,
		Originate: "App.handlePong",
	}
}
