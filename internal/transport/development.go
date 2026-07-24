package transport

import "github.com/Lakshay309/bitgopher/internal/peer"

// development
func (t *TCPTransport) ConnectionCount() int {
	resp := make(chan peer.PeerResponse)

	t.peerManager.PeerEventChan <- peer.PeerEvent{
		Type:     peer.GetConnectionCountEvent,
		Response: resp,
	}
	result := <-resp

	return result.Count
}

func (t *TCPTransport) getPeers() []peer.PeerInfo {
	resp := make(chan peer.PeerResponse)

	t.peerManager.PeerEventChan <- peer.PeerEvent{
		Type:     peer.GetPeersEvent,
		Response: resp,
	}

	result := <-resp
	return result.Peers
}
