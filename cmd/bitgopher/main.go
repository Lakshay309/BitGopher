package main

import (
	"fmt"
	"net"
	"time"

	"github.com/Lakshay309/bitgopher/internal/discovery"
	"github.com/Lakshay309/bitgopher/internal/peer"
	"golang.org/x/net/ipv4"
)

const multicastAddr = "239.255.10.10:9999"

func receiver(a int) {
	addr, err := net.ResolveUDPAddr("udp4", multicastAddr)
	if err != nil {
		panic(err)
	}

	iface, err := net.InterfaceByName("lo")
	if err != nil {
		panic(err)
	}
	fmt.Println("Interface:", iface.Name)
	fmt.Println("Index:", iface.Index)
	fmt.Println("Flags:", iface.Flags)
	conn, err := net.ListenMulticastUDP("udp4", iface, addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	buf := make([]byte, 1024)

	fmt.Println("Receiver started...")

	for {
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Println(err)
			continue
		}

		fmt.Printf("%d [RECV] %s -> %s\n", a, src, string(buf[:n]))
	}
}

func sender(a int) {
	addr, err := net.ResolveUDPAddr("udp4", multicastAddr)
	if err != nil {
		panic(err)
	}

	iface, err := net.InterfaceByName("lo")
	if err != nil {
		panic(err)
	}
	localAddr := &net.UDPAddr{
		IP: net.ParseIP("127.0.0.1"),
		Port: 0,
	}

	conn, err := net.DialUDP("udp4", localAddr, addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	p := ipv4.NewPacketConn(conn)

	if err := p.SetMulticastInterface(iface); err != nil {
		panic(err)
	}

	if err := p.SetMulticastLoopback(true); err != nil {
		panic(err)
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	fmt.Println("Sender started...")

	for range ticker.C {
		msg := []byte(fmt.Sprintf("HELLO%d", a))

		_, err := conn.Write(msg)
		if err != nil {
			fmt.Println(err)
			continue
		}

		fmt.Printf("%d [SEND] %s\n", a, msg)
	}
}

func main() {
	// tcp server starts first 
	discoveryServer := discovery.NewUdpServer(":8081","",3)

	// creating the peer
	Peer := peer.NewLocalPeer(":8081","")

	discoveryServer.PeerID = Peer.PeerID

	go discoveryServer.Broadcast()

	go discoveryServer.Receiver()

	select {}
}
