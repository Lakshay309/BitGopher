package pkg

import "net"


// peer is the interface represents remote node
type Peer interface{
	net.Conn
}

// Transport interface handles the communcation 
// between the node in the network
type Transport interface{
	//to out our address
	Addr() string 
	Dial(string) error
	ListenAndAccept() error
}

// Broadcast interface handles remote node discovery
type Brodcaster interface{
	// periodic outbound 
	broadcast() error
	// Inbound UDP server Listening for neighbour
	ListenForBroadcasts() error
}