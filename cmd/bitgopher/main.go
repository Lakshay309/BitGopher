package main

import (
	"time"

	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/Lakshay309/bitgopher/internal/discovery"
	"github.com/Lakshay309/bitgopher/internal/peer"
)


func main() {
	// tcp server starts first
	var i common.DiscoveryMode = 0
	port :=":8082"

	peerManager := peer.NewPeerManager(i,10)

	go peerManager.Run()

	discoveryServer := discovery.NewUdpServer(port, i,peerManager.DiscoveryChan,peerManager.RemoverChan)

	discoveryServer.PeerID = peerManager.Self.ID

	go discoveryServer.Broadcast()

	go discoveryServer.Receiver()

	time.Sleep(10 * time.Second)

	discoveryServer.Stop()

	select {}
}
