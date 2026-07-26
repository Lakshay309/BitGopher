package app

import (
	"log/slog"

	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/Lakshay309/bitgopher/internal/discovery"
	"github.com/Lakshay309/bitgopher/internal/peer"
	"github.com/Lakshay309/bitgopher/internal/transport"
	"github.com/google/uuid"
)

type App struct {
	transport   *transport.TCPTransport
	peerManager *peer.PeerManager
	discovery   *discovery.UdpServer
}

func NewApp() (*App, error) {
	// TODO: how to ask user the mode and password
	var mode common.DiscoveryMode = 1
	password := "hu"
	peerManager := peer.NewPeerManager(mode)
	transport, err := transport.NewTCPTransport(peerManager)
	if err != nil {
		return nil, err
	}
	udpserver, err := discovery.NewUdpServer(peerManager, password)
	if err != nil {
		return nil, err
	}
	return &App{
		transport:   transport,
		discovery:   udpserver,
		peerManager: peerManager,
	}, nil
}

func (a *App) Start() error {
	// start tcp server
	if err := a.transport.Start(); err != nil {
		slog.Error("[app start]", "err", err)
		return err
	}

	// run loop peermanager
	go a.peerManager.Run()

	// now start discovery server
	if err := a.discovery.Start(); err != nil {
		slog.Error("[app start]", "err", err)
		return err
	}
	return nil
}

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
		Type:     peer.GetPeersEvent,
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
	if peer!=nil && peer.Conn!=nil{
		return nil
	}

	return a.transport.Connect(tcpAddr)
}
