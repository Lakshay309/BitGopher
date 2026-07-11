package discovery

import (
	"fmt"
	"log"
	"net"
	"time"
)

type PeerInfoForUdpServer struct {
	PeerID  string
	TCPAddr string
}

type UdpServer struct {
	ListenAddr string
	TCPAddr    string
	// peerInfo   PeerInfoForUdpServer

	Listener *net.UDPConn
}

func NewUdpServer(ListenAddr string, TCPAddr string) *UdpServer {
	return &UdpServer{
		ListenAddr: ListenAddr,
		TCPAddr:    TCPAddr,
	}
}

// start the udp server
func (u *UdpServer) Start() error {
	// resolve the UDP address to bind to
	addr, err := net.ResolveUDPAddr("udp", u.ListenAddr)
	if err != nil {
		log.Fatalf("Failed to resolve UDP address: %v", err)
		return err
	}

	// start listening for incoming UDP packets
	u.Listener, err = net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("Failed to start UDP server: %v", err)
		return err
	}

	log.Printf("UDP Server listening on %s\n ", u.ListenAddr)

	// start an accept loop
	go u.startAcceptLoop()

	return nil
}

func (u *UdpServer) startAcceptLoop() error {
	buffer := make([]byte, 4096)

	for {
		n, addr, err := u.Listener.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("UDP read error: %v", err)
			continue
		}
		log.Printf(
			"Received %d bytes from %s: %s",
			n,
			addr.String(),
			string(buffer[:n]),
		)
	}
}

func (u *UdpServer) Broadcast() error {
	data := fmt.Appendf(nil, "HELLO|localhost%s", u.TCPAddr)
	go u.broadcastLoop(data)
	return nil
}

func (u *UdpServer) broadcastLoop(data []byte) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	// version 1
	addr, err := net.ResolveUDPAddr("udp", u.ListenAddr)
	if err != nil {
		log.Println(err)
	}
	for range ticker.C {
		_, err := u.Listener.WriteToUDP(data, addr)
		if err != nil {
			log.Println(err)
			continue
		}
		log.Println("broadcasted:", string(data))
	}
}

func (u *UdpServer) Stop() error {
	if u.Listener != nil {
		return u.Listener.Close()
	}

	return nil
}
