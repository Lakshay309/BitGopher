package main

import (
	"github.com/Lakshay309/bitgopher/internal/discovery"
	"github.com/Lakshay309/bitgopher/internal/peer"
)

func main() {
	// tcp server starts first
	discoveryServer := discovery.NewUdpServer(":8084", 1)

	// creating the peer
	Peer := peer.NewLocalPeer(":8084", "")

	discoveryServer.PeerID = Peer.PeerID

	go discoveryServer.Broadcast()

	go discoveryServer.Receiver()

	select {}
}
