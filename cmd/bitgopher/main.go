package main

import (
	"fmt"
	"time"

	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/Lakshay309/bitgopher/internal/discovery"
	"github.com/Lakshay309/bitgopher/internal/peer"
	"github.com/Lakshay309/bitgopher/internal/transport"
	"github.com/google/uuid"
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

	tcp1, err := transport.NewTCPTransport(":8080", uuid.New())
	if err != nil {
		fmt.Println(err)
	}
	tcp2, err := transport.NewTCPTransport(":8081", uuid.New())
	if err != nil {
		fmt.Println(err)
	}
	tcp1.Start()
	tcp2.Start()
	time.Sleep(2 * time.Second)

	tcp1.Connect("localhost:8081")

	time.Sleep(5 * time.Second)

	fmt.Println("TCP1 Connections:", tcp1.ConnectionCount())
	fmt.Println("TCP2 Connections:", tcp2.ConnectionCount())
	// discoveryServer.Stop()

	select {}
}
