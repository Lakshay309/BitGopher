package app

import (
	"log/slog"

	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/Lakshay309/bitgopher/internal/discovery"
	"github.com/Lakshay309/bitgopher/internal/peer"
	"github.com/Lakshay309/bitgopher/internal/transport"
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
		transport: transport,
		discovery: udpserver,
		peerManager: peerManager,
	}, nil
}

func (a *App) Start()error{
	// start tcp server 
	if err:=a.transport.Start();err!=nil{
		slog.Error("[app start]","err",err)
		return err
	}

	// run loop peermanager
	go a.peerManager.Run()

	// now start discovery server
	if err := a.discovery.Start(); err != nil {
        slog.Error("[app start]","err",err)
		return err
    }
	return nil
}
