package peer

import (
	"log"
	"time"

	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/google/uuid"
)

type PeerInfo struct {
	ID        string
	TCPAddr   string
	LastSeen  time.Time
	Discovery common.DiscoveryMode
}

type PeerManager struct {
	Self          PeerInfo
	Peers         map[string]*PeerInfo
	DiscoveryChan chan PeerInfo
}

func NewPeerManager(mode common.DiscoveryMode, channelBuffer int) *PeerManager {
	id := uuid.New().String()
	selfInfo := PeerInfo{ID: id, Discovery: mode}

	return &PeerManager{
		Self:          selfInfo,
		Peers:         make(map[string]*PeerInfo),
		DiscoveryChan: make(chan PeerInfo, channelBuffer),
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
		case peer := <-pm.DiscoveryChan:
			if existing, ok := pm.Peers[peer.ID]; ok {
				existing.LastSeen = peer.LastSeen
				existing.TCPAddr = peer.TCPAddr
				existing.Discovery = peer.Discovery

				continue
			}
			log.Printf("New peer discovered: %s (%s)", peer.ID, peer.TCPAddr)
			pm.Peers[peer.ID] = &peer

		case <-ticker.C:
			for id, peer := range pm.Peers {
				if peer.ID == pm.Self.ID{
					continue
				}
				if time.Since(peer.LastSeen) > peerTimeOut {
					log.Printf("Peer expired: %s (%s)", peer.ID, peer.TCPAddr)
					delete(pm.Peers, id)
				}
			}
		}
	}
}
