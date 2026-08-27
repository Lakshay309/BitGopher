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


/*
* or we can change what does the blacklist entirely means it means now if peer is balcklisted it is permenant for that session(currently) like the user dont want to connect to that peer and if we simply disconnect it is just like we want to remove the current connection medium something like that and the discovery layer will discover the peer again just after the the peer was disconnected
*/

type PeerInfo struct {
	ID           uuid.UUID
	TCPAddr      string
	LastSeen     time.Time
	LastActivity time.Time
	Discovery    common.DiscoveryMode

	Connected bool
	Conn      net.Conn
	// TODO: add write chan here think about it now 

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

	HandleBlackListPeer
	CheckBlackListPeer
	GetBlackListPeer

)
const (
	PeerEventChanSize = 128
	BlackListPeerTimeoutTime = 5
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

	// TODO: update this in the constructor
	BlackListPeers map[uuid.UUID]struct{}

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
		BlackListPeers: make(map[uuid.UUID]struct{}),
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
			pm.handlePeerEvent(event)

		case <-ticker.C:
			pm.checkPeerTimeouts(peerTimeOut, peerConnectionTimeOut)
		}
	}
}

func (pm *PeerManager) handlePeerEvent(event PeerEvent) {

	switch event.Type {

	case DiscoveryEvent:
		pm.handleDiscoveryEvent(event)

	case RemovePeerEvent:
		pm.handleRemovePeerEvent(event)

	case SetConnectionEvent:
		pm.handleSetConnectionEvent(event)

	case RemoveConnectionEvent:
		pm.handleRemoveConnectionEvent(event)

	case GetPeersEvent:
		pm.handleGetPeersEvent(event)

	case GetPeerEvent:
		pm.handleGetPeerEvent(event)

	case GetConnectionCountEvent:
		pm.handleGetConnectionCountEvent(event)

	case SetLastActivity:
		pm.handleSetLastActivity(event)
	
	case HandleBlackListPeer:
		pm.handleBlackListPeer(event)
	
	case CheckBlackListPeer:
		pm.handleCheckBlackListPeer(event)
	case GetBlackListPeer:
		pm.handleGetBlackListPeer(event)
	}
}

// TODO: complete this 
func (pm *PeerManager) handleGetBlackListPeer(event PeerEvent) {
	blackListPeers := make([]PeerInfo, len(pm.BlackListPeers))

	i := 0
	for peer := range pm.BlackListPeers {
		blackListPeers[i] = PeerInfo{ID: peer}
		i++
	}

	event.Response <- PeerResponse{
		Peers: blackListPeers,
	}
}


func (pm *PeerManager) handleCheckBlackListPeer(event PeerEvent){
	uid := event.Command.Peer.ID
	if _,ok:=pm.BlackListPeers[uid];ok{
		event.Response<-PeerResponse{
			Count: 1,
		}
		return
	}
	event.Response<-PeerResponse{
			Count:0,
		}
}


func (pm *PeerManager) handleBlackListPeer(event PeerEvent){
	uid:=event.Command.Peer.ID
	if _,ok:=pm.BlackListPeers[uid];ok{
		delete(pm.BlackListPeers,uid)
		return
	}
	delete(pm.Peers,uid)
	pm.BlackListPeers[uid] =struct{}{}
}

func (pm *PeerManager) handleDiscoveryEvent(event PeerEvent) {

	peer := event.Command.Peer

	if _,ok:=pm.BlackListPeers[peer.ID];ok{
		return
	}

	if existing, ok := pm.Peers[peer.ID]; ok {
		existing.LastSeen = peer.LastSeen
		existing.TCPAddr = peer.TCPAddr
		existing.Discovery = peer.Discovery
		return
	}

	peer.LastActivity = time.Now()

	log.Printf("New peer discovered: %s (%s)", peer.ID, peer.TCPAddr)
	pm.Peers[peer.ID] = &peer
}


func (pm *PeerManager) handleRemovePeerEvent(event PeerEvent) {

	peer, ok := pm.Peers[event.Command.Peer.ID]
	if !ok {
		return
	}

	log.Printf("Removing peer: %s", peer.ID)

	if peer.Conn != nil {
		_ = peer.Conn.Close()
	}

	// TODO: blacklist peers
	// pm.handleAddPeerInBlackListPeer(event)
	delete(pm.Peers, peer.ID)
}

func (pm *PeerManager) handleSetConnectionEvent(event PeerEvent) {

	peer, ok := pm.Peers[event.Command.Peer.ID]
	if !ok {
		event.Response <- PeerResponse{
			Err: fmt.Errorf("peer not found"),
		}
		return
	}

	if peer.Conn != nil {
		event.Response <- PeerResponse{
			Err: fmt.Errorf("duplicate connection"),
		}
		return
	}

	peer.Conn = event.Command.Peer.Conn
	peer.Connected = true

	event.Response <- PeerResponse{}
}

func (pm *PeerManager) handleRemoveConnectionEvent(event PeerEvent) {

	peer, ok := pm.Peers[event.Command.Peer.ID]
	if !ok {
		return
	}

	if event.Command.Peer.Conn != nil && event.Command.Peer.Conn != peer.Conn {
		return
	}

	if peer.Conn != nil {
		peer.Conn.Close()
	}

	peer.Conn = nil
	peer.Connected = false
}

func (pm *PeerManager) handleGetPeersEvent(event PeerEvent) {

	peers := make([]PeerInfo, 0, len(pm.Peers))

	for _, peer := range pm.Peers {
		peers = append(peers, *peer)
	}

	event.Response <- PeerResponse{
		Peers: peers,
	}
}

func (pm *PeerManager) handleGetPeerEvent(event PeerEvent) {

	peer, ok := pm.Peers[event.Query.PeerID]
	if !ok {
		event.Response <- PeerResponse{
			Err: errors.New("peer not found"),
		}
		return
	}

	copy := *peer

	event.Response <- PeerResponse{
		Peer: &copy,
	}
}

func (pm *PeerManager) handleGetConnectionCountEvent(event PeerEvent) {

	event.Response <- PeerResponse{
		Count: len(pm.Peers),
	}
}

func (pm *PeerManager) handleSetLastActivity(event PeerEvent) {

	peer, ok := pm.Peers[event.Command.Peer.ID]
	if !ok {
		return
	}

	peer.LastActivity = time.Now()
}


func (pm *PeerManager) checkPeerTimeouts(peerTimeOut time.Duration, peerConnectionTimeOut time.Duration) {

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

		if time.Since(peer.LastActivity) > peerConnectionTimeOut &&
			peer.Connected &&
			peer.Conn != nil {

			log.Printf("Peer connection Timeout: %s", peer.ID)

			if peer.Conn != nil {
				if err := peer.Conn.Close(); err != nil {
					log.Printf(
						"Peer Connection TimeoutError: (closing connection) %s ",
						err,
					)
				}

				peer.Connected = false
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


