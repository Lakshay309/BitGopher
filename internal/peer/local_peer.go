package peer

import "github.com/google/uuid"

type localPeer struct{
	PeerID string

	// initiatel after wards
	
	TCPServerAddr string
	UDPServerAddr string
} 


func NewLocalPeer(tcpAddr string,udpAddr string)*localPeer{
	id := uuid.New().String()

	return &localPeer{
		PeerID: id,
		TCPServerAddr: tcpAddr,
		UDPServerAddr: udpAddr,
	}
}