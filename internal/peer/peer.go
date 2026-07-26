package peer

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/google/uuid"
)

type PeerInfo struct {
	ID           uuid.UUID
	TCPAddr      string
	LastSeen     time.Time
	LastActivity time.Time
	Discovery    common.DiscoveryMode

	Connected bool
	Conn      net.Conn
}

type PeerEventType byte

const (
	DiscoveryEvent PeerEventType = iota
	RemovePeerEvent
	SetConnectionEvent
	RemoveConnectionEvent

	GetPeersEvent
	GetPeerEvent
	GetConnectionCountEvent
	SetLastActivity
)
const (
	PeerEventChanSize = 128
)

type PeerCommand struct {
	Peer PeerInfo
}

type PeerQuery struct {
	PeerID uuid.UUID
}

type PeerResponse struct {
	Peers []PeerInfo
	Peer  *PeerInfo
	Count int
	Err   error
}

type PeerEvent struct {
	Type PeerEventType

	Command  PeerCommand
	Query    PeerQuery
	Response chan PeerResponse
}

type PeerManager struct {
	Self  PeerInfo
	Peers map[uuid.UUID]*PeerInfo

	PeerEventChan chan PeerEvent
}

func NewPeerManager(mode common.DiscoveryMode) *PeerManager {
	id := uuid.New()

	selfInfo := PeerInfo{
		ID:        id,
		Discovery: mode,
	}

	return &PeerManager{
		Self:          selfInfo,
		Peers:         make(map[uuid.UUID]*PeerInfo),
		PeerEventChan: make(chan PeerEvent, PeerEventChanSize),
	}
}

func (pm *PeerManager) Run() {
	
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	peerTimeOut := 2 * time.Minute
	peerConnectionTimeOut := 10 * time.Minute

	for {
		select {

		case event := <-pm.PeerEventChan:

			switch event.Type {

			case DiscoveryEvent:
				peer := event.Command.Peer

				if existing, ok := pm.Peers[peer.ID]; ok {
					existing.LastSeen = peer.LastSeen
					existing.TCPAddr = peer.TCPAddr
					existing.Discovery = peer.Discovery
					continue
				}

				log.Printf("New peer discovered: %s (%s)", peer.ID, peer.TCPAddr)
				pm.Peers[peer.ID] = &peer

			case RemovePeerEvent:
				peer, ok := pm.Peers[event.Command.Peer.ID]
				if !ok {
					continue
				}

				log.Printf("Removing peer: %s", peer.ID)

				if peer.Conn != nil {
					_ = peer.Conn.Close()
				}

				delete(pm.Peers, peer.ID)

			case SetConnectionEvent:
				peer, ok := pm.Peers[event.Command.Peer.ID]
				if !ok {
					event.Response<-PeerResponse{
						Err: fmt.Errorf("peer not found"),
					}
					continue
				}
				if peer.Conn!=nil {
					event.Response<-PeerResponse{
						Err: fmt.Errorf("duplicate connection"),
					}
					continue
				}

				peer.Conn = event.Command.Peer.Conn
				peer.Connected = true
				event.Response <- PeerResponse{}

			case RemoveConnectionEvent:
				peer, ok := pm.Peers[event.Command.Peer.ID]
				if !ok {
					continue
				}
				if event.Command.Peer.Conn!=nil && event.Command.Peer.Conn != peer.Conn{
					continue
				}
				if peer.Conn!=nil{
					peer.Conn.Close()
				}

				peer.Conn = nil
				peer.Connected = false

			case GetPeersEvent:
				peers := make([]PeerInfo, 0, len(pm.Peers))

				for _, peer := range pm.Peers {
					peers = append(peers, *peer)
				}

				event.Response <- PeerResponse{
					Peers: peers,
				}

			case GetPeerEvent:
				peer, ok := pm.Peers[event.Query.PeerID]
				if !ok {
					event.Response <- PeerResponse{
						Err: errors.New("peer not found"),
					}
					continue
				}

				copy := *peer

				event.Response <- PeerResponse{
					Peer: &copy,
				}
			case GetConnectionCountEvent:
				event.Response <- PeerResponse{
					Count: len(pm.Peers),
				}
			case SetLastActivity:
				peer, ok := pm.Peers[event.Command.Peer.ID]
				if !ok {
					continue
				}
				peer.LastActivity = time.Now()

			}

		case <-ticker.C:
			for id, peer := range pm.Peers {
				if peer.ID == pm.Self.ID {
					continue
				}

				if time.Since(peer.LastSeen) > peerTimeOut {
					log.Printf("Peer expired: %s (%s)", peer.ID, peer.TCPAddr)

					if peer.Conn != nil {
						_ = peer.Conn.Close()
					}

					delete(pm.Peers, id)
					continue
				}
				
				if time.Since(peer.LastActivity) > peerConnectionTimeOut && peer.Connected && peer.Conn!=nil {
					log.Printf("Peer connection Timeout: %s", peer.ID)

					if peer.Conn != nil {
						if err := peer.Conn.Close(); err != nil {
							log.Printf("Peer Connection TimeoutError: (closing connection) %s ", err)
						}
						peer.Connected = false
					}
				}
			}
		}
	}
}


// setter

func (pm *PeerManager) SetTCPAddr(addr string) {
    pm.Self.TCPAddr = addr
}

func (pm *PeerManager) SetSelfInfo(tcpAddr string) {
	pm.Self.TCPAddr = tcpAddr
}