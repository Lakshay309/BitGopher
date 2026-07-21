package main

import (
	"fmt"
	"time"

	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/Lakshay309/bitgopher/internal/discovery"
	"github.com/Lakshay309/bitgopher/internal/peer"
)

func main() {
	// tcp server starts first
	var i common.DiscoveryMode = 1
	port := ":8082"

	peerManager := peer.NewPeerManager(i, 10)

	go peerManager.Run()

	discoveryServer, err := discovery.NewUdpServer(port, i, peerManager.DiscoveryChan, peerManager.RemoverChan, "hu")

	if err != nil {
		fmt.Println("error creating udp server")
	}

	discoveryServer.PeerID = peerManager.Self.ID

	go discoveryServer.Start()

	time.Sleep(10 * time.Second)

	discoveryServer.Stop()

	select {}
}
