package discovery

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/Lakshay309/bitgopher/internal/peer"
	"golang.org/x/net/ipv4"
)

// constants
type PacketType byte

const (
	multicastAddr = "239.255.10.10:9999"
	UUIDSize      = 16
)

const (
	Hello PacketType = iota + 1
	Bye
)

var salt = []byte("BitGopher-v1")

type UdpServer struct {
	peerManager *peer.PeerManager

	exit chan struct{}

	recvConn *net.UDPConn

	gcm cipher.AEAD
}

func NewUdpServer(pm *peer.PeerManager, password string) (*UdpServer, error) {

	key := DeriveKey(password)

	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return &UdpServer{
		peerManager: pm,
		exit: make(chan struct{}),
		gcm:  gcm,
	}, nil
}

// start the udp server
func (u *UdpServer) Start() error {

	// start an Receiver ( different for windows and linux )
	go u.Receiver()

	// starting broadcast
	go u.Broadcast()

	return nil
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

	// slog.Info(
	// 	"Using interface",
	// 	"name", iface.Name,
	// 	"flags", iface.Flags.String(),
	// )

	ip, err := getIPv4(iface)
	if err != nil {
		panic(err)
	}

	localAddr := net.UDPAddr{
		IP:   ip,
		Port: 0,
	}

	tcpAddr := net.JoinHostPort(ip.String(), strings.TrimPrefix(u.peerManager.Self.TCPAddr, ":"))

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

	// slog.Info("[broadcastloop] Sender started...")

	for {
		select {
		case <-ticker.C:
			if err := u.sendPacket(tcpAddr, Hello, conn); err != nil {
				slog.Error("[broadcastLoop]", "[loop]", err.Error())
			}
		case <-u.exit:
			if err := u.sendPacket(tcpAddr, Bye, conn); err != nil {
				slog.Error("[broadcastLoop]", "[loop]", err.Error())
			}
			return nil
		}
	}
}

func (u *UdpServer) sendPacket(tcpAddr string, message PacketType, conn *net.UDPConn) error {

	data := make([]byte, 0, 2+len(tcpAddr)+UUIDSize)

	data = append(data, byte(message))

	data = append(data, byte(len(tcpAddr)))
	data = append(data, tcpAddr...)

	data = append(data, u.peerManager.Self.ID[:]...)

	//  Encrypt the data before writing it on connection
	packet, err := u.encryptData(data)
	if err != nil {
		return err
	}

	_, err = conn.Write(packet)
	return err
}

func (u *UdpServer) Stop() error {
	//  we will broadcast that we are closing the tcp server if user actually stop the server then this fucntion must be called and this function will notify other peer that this peer is actually going to sleep
	close(u.exit)

	if u.recvConn != nil {
		return u.recvConn.Close()
	}

	return nil
}

// TODO: currently this is not the best way to get LAN as it return first suitable interface ( like a system can have a DOCKER or VMware running that can cause issue as it can choose one of those interface so change this before conpletion of the project)
func (u *UdpServer) getInterface() (*net.Interface, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	switch u.peerManager.Self.Discovery {
	case common.Local:
		for _, iface := range interfaces {
			if iface.Flags&net.FlagUp == 0 {
				continue
			}
			if iface.Flags&net.FlagLoopback != 0 {
				return &iface, nil
			}
		}
	case common.LAN:
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
