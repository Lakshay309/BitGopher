package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/Lakshay309/bitgopher/internal/discovery"
	"github.com/Lakshay309/bitgopher/internal/peer"
	"github.com/Lakshay309/bitgopher/internal/transport"
)

func main() {
	// tcp server starts first
	var i common.DiscoveryMode = 1
	port := ":8086"

	peerManager := peer.NewPeerManager(i, 10,port)

	go peerManager.Run()

	discoveryServer, err := discovery.NewUdpServer(port, i, peerManager.PeerEventChan, "hello")

	if err != nil {
		fmt.Println("error creating udp server")
	}

	discoveryServer.PeerID = peerManager.Self.ID

	tcp, err := transport.NewTCPTransport(peerManager)
	if err != nil {
		fmt.Println(err)
	}

	if err := tcp.Start(); err != nil {
		log.Fatalf("failed to start TCP server: %v", err)
	}

	time.Sleep(2 * time.Second)

	go discoveryServer.Start()

	// so i can start 2 instance and they both able to communicate with each other

	for tcp.ConnectionCount() == 0 {
		time.Sleep(100 * time.Millisecond)
	}

	if err := tcp.Connect(""); err != nil {
		log.Println(err)
	}

	time.Sleep(5 * time.Second)
	// get peers 
	peers := tcp.GetPeers()
	
	// sending the ping command
	for _,peer := range peers{
		if peer.ID != peerManager.Self.ID{
			tcp.WriteChan <-transport.WriteCommand{
				Conn:peer.Conn,
				Packet: transport.Packet{
					Type:transport.PingPacket,
				},
			}
		}
	}


	fmt.Println("TCP1 Connections:", tcp.ConnectionCount())

	select {}
}
