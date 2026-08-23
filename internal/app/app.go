package app

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/Lakshay309/bitgopher/internal/discovery"
	"github.com/Lakshay309/bitgopher/internal/filemanager"
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
	fileManager *filemanager.FileManager
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
	return &App{
		transport:   transport,
		discovery:   udpserver,
		peerManager: peerManager,
		exit:        make(chan struct{}),
		UiLogChan:   make(chan UILog, UILogChanSize),
		UiChan:      make(chan UICommand, UIChanSize),
	}, nil
}


func (a *App) Start() error {
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

// TODO: search functionality 
func(a *App) SearchForAFile(fileName string){
	fileName=strings.Trim(fileName," ")
	if len(fileName)==0{
		return
	}
	// a.
}


/* TODO: what do we have to do for file handling part 

*TODO : peer after disconnect gets re connect solve that


* create an api that will can send request to other peer for the search of particular file do it exist with them or not 

*if that particular file Exist we have to have send info about the file metadata

* if not exist we still have to response negative to the peer 

* we also have to maintain a filetracker that will see who have which file with them and store relivant info about the file in that peer(reciever peer)

* then we will have a option to to get a file using the name that is asociated with it we will take hash from the filetracker and then send that hash to the peer that have that particular file like we wnat that file

* we also have to build relevant protocol for the file transfer how the file trnafer protocol should look 

* we also ahve to create some mechanism that will  recieve the file from the peer 


* * what we have currently in filemanager

* we have a way to get the file information for a particular hash ( we can get the metadata about the file which can be used to steam the file)
* we have a functionality to search the file using name and stuff in filemanager
* remove a particular file using the fileshash
* get all the file we are seeding locally
* we also have a complete funcitonlity to seed a file using the file path and some metadata fields 

*/


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


