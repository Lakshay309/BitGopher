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

// constants
const multicastAddr = "239.255.10.10:9999"

type DiscoveryMode int

const (
	Local DiscoveryMode = iota
	LAN
	// WAN will have its own struct with same inferface discovery
)

type UdpServer struct {
	// ListenAddr string
	TCPAddr string
	// map peer_id -> TCPAddr
	PeerInfo map[string]string
	// should be added later after creating the peer
	PeerID string

	discoveryMode DiscoveryMode
}

func NewUdpServer(TCPAddr string, discoveryMode DiscoveryMode) *UdpServer {
	return &UdpServer{
		TCPAddr:       TCPAddr,
		PeerInfo:      make(map[string]string),
		discoveryMode: discoveryMode,
	}
}

// start the udp server
func (u *UdpServer) Start() error {

	// start an Receiver loop
	go u.Receiver()

	// starting broadcast
	go u.Broadcast()

	return nil
}

func (u *UdpServer) Receiver() error {
	buffer := make([]byte, 256)

	addr, err := net.ResolveUDPAddr("udp4", multicastAddr)
	if err != nil {
		panic(err)
	}

	iface, err := u.getInterface()
	if err != nil {
		panic(err)
	}
	conn, err := net.ListenMulticastUDP("udp4", iface, addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// receiver loop here
	for {
		n, addr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			slog.Info("[StartAcceptLoop]", "[error-msg]", err.Error())
			continue
		}
		info := strings.Split(string(buffer[:n]), "|")

		if len(info) < 3 {
			continue
		}

		if u.PeerID == info[2] {
			continue
		}

		if _, ok := u.PeerInfo[info[2]]; !ok {
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

	
	addr, err := net.ResolveUDPAddr("udp4", multicastAddr)
	if err != nil {
		panic(err)
	}
	
	// this will change depending on platform
	iface, err := u.getInterface()
	if err != nil {
		panic(err)
	}
	slog.Info(
    "Using interface",
    "name", iface.Name,
    "flags", iface.Flags.String(),
)
	
	ip, err := getIPv4(iface)
	if err != nil {
		panic(err)
	}
	
	localAddr := net.UDPAddr{
		IP:   ip,
		Port: 0,
	}

	tcpAddr := net.JoinHostPort(ip.String(), strings.TrimPrefix(u.TCPAddr, ":"))

	data := fmt.Appendf(nil,
		"HELLO|%s|%s",
		tcpAddr,
		u.PeerID,
	)
	
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
			slog.Error("[broadcastLoop]", "[loop]", err.Error())
			continue
		}
	}
	return nil
}

func (u *UdpServer) Stop() error {
	// if u.Listener != nil {
	// 	return u.Listener.Close()
	// }

	return nil
}

//TODO: currently this is not the best way to get LAN as it return first suitable interface ( like a system can have a DOCKER or VMware running that can cause issue as it can choose one of those interface so change this before conpletion of the project) 
func (u *UdpServer) getInterface() (*net.Interface, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	switch u.discoveryMode {
	case Local:
		for _, iface := range interfaces {
			if iface.Flags&net.FlagUp == 0 {
				continue
			}
			if iface.Flags&net.FlagLoopback != 0 {
				return &iface, nil
			}
		}
	case LAN:
		for _, iface := range interfaces {
			if iface.Flags&net.FlagUp == 0 {
				continue
			}
			if iface.Flags&net.FlagLoopback != 0 {
				continue
			}
			if iface.Flags&net.FlagMulticast == 0 {
				continue
			}

			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				ipNet, ok := addr.(*net.IPNet)
				if !ok {
					continue
				}
				if ipNet.IP.To4() != nil {
					return &iface, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("no suitable interface found")
}

func getIPv4(iface *net.Interface) (net.IP, error) {
	addrs, err := iface.Addrs()

	if err != nil {
		return nil, err
	}

	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}

		ip := ipNet.IP.To4()
		if ip != nil {
			return ip, nil
		}
	}
	return nil, fmt.Errorf("no ipv4 found")
}
