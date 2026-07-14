package discovery

import (
	"fmt"
	"log"
	"log/slog"
	"net"
	"strings"
	"time"

	"golang.org/x/net/ipv4"
)

type UdpServer struct {
	// ListenAddr string
	TCPAddr    string
	// map peer_id -> TCPAddr
	PeerInfo map[string]string
	// should be added later after creating the peer
	PeerID string
	// will be constant thorough the app
	MulticastAddr string

	// Listener *net.UDPConn

	debugger int
}

func NewUdpServer(TCPAddr string, MulticastAddr string, debugger int) *UdpServer {
	if len(MulticastAddr) == 0 {
		MulticastAddr = "239.255.10.10:9999"
	}
	return &UdpServer{
		TCPAddr:       TCPAddr,
		PeerInfo:      make(map[string]string),
		MulticastAddr: MulticastAddr,
		debugger:      debugger,
	}
}

// start the udp server
func (u *UdpServer) Start() error {

	// start an Reciver loop
	go u.Receiver()

	// starting broadcast
	go u.Broadcast()

	return nil
}

func (u *UdpServer) Receiver() error {
	buffer := make([]byte, 256)

	addr, err := net.ResolveUDPAddr("udp4", u.MulticastAddr)
	if err != nil {
		panic(err)
	}

	iface, err := net.InterfaceByName("lo")
	if err != nil {
		panic(err)
	}
	conn, err := net.ListenMulticastUDP("udp4", iface, addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// reciever loop here
	for {
		n, addr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			slog.Info("[StartAcceptLoop]", "[error-msg]", err.Error())
			continue
		}
		info := strings.Split(string(buffer[:n]),"|")
		
		if len(info) < 3{
			continue
		}
		
		if u.PeerID == info[2]{
			continue
		}

		if _,ok :=u.PeerInfo[info[2]]; !ok{
			log.Printf(
				"Received %d bytes from %s: %s",
				n,
				addr.String(),
				string(buffer[:n]),
			)
		}
		
		u.PeerInfo[info[2]] = info[1]
	}
}

func (u *UdpServer) Broadcast() error {

	data := fmt.Appendf(nil, "HELLO|localhost%s|%s", u.TCPAddr, u.PeerID)

	addr, err := net.ResolveUDPAddr("udp4", u.MulticastAddr)
	if err != nil {
		panic(err)
	}

	// this will change depending on platform
	iface, err := net.InterfaceByName("lo")
	if err != nil {
		panic(err)
	}

	localAddr := net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 0,
	}

	conn, err := net.DialUDP("udp4", &localAddr, addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	packetConnector := ipv4.NewPacketConn(conn)

	if err := packetConnector.SetMulticastInterface(iface); err != nil {
		panic(err)
	}

	if err := packetConnector.SetMulticastLoopback(true); err != nil {
		panic(err)
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	slog.Info("[broadcastloop] Sender started...")

	for range ticker.C {
		_, err := conn.Write(data)
		if err != nil {
			slog.Error("[boardcastLoop]", "[loop]", err.Error())
			continue
		}
		slog.Info(fmt.Sprintf("[broadcastloop][%d]", u.debugger))

	}
	return nil
}

func (u *UdpServer) Stop() error {
	// if u.Listener != nil {
	// 	return u.Listener.Close()
	// }

	return nil
}
