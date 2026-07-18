package main

import (
	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/Lakshay309/bitgopher/internal/discovery"
	"github.com/Lakshay309/bitgopher/internal/peer"
)




func main() {
	// tcp server starts first
	var i common.DiscoveryMode = 0
	port :=":8081"

	peerManager := peer.NewPeerManager(i,10)

	go peerManager.Run()

	discoveryServer := discovery.NewUdpServer(port, i,peerManager.DiscoveryChan)

	discoveryServer.PeerID = peerManager.Self.ID

	go discoveryServer.Broadcast()

	go discoveryServer.Receiver()

	select {}
}
