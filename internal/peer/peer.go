package peer

import (
	"errors"
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

func NewPeerManager(mode common.DiscoveryMode, channelBuffer int, port string) *PeerManager {
	id := uuid.New()

	selfInfo := PeerInfo{
		ID:        id,
		TCPAddr:   port,
		Discovery: mode,
	}

	return &PeerManager{
		Self:          selfInfo,
		Peers:         make(map[uuid.UUID]*PeerInfo),
		PeerEventChan: make(chan PeerEvent, channelBuffer),
	}
}

func (pm *PeerManager) SetSelfInfo(tcpAddr string) {
	pm.Self.TCPAddr = tcpAddr
}

func (pm *PeerManager) Run() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	peerTimeOut := 2 * time.Minute

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
					continue
				}

				peer.Conn = event.Command.Peer.Conn
				peer.Connected = true

			case RemoveConnectionEvent:
				peer, ok := pm.Peers[event.Command.Peer.ID]
				if !ok {
					continue
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
				}
			}
		}
	}
}
