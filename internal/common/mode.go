package common


type DiscoveryMode int

const (
	Local DiscoveryMode = iota +1
	LAN
	// WAN // will have its own struct with same inferface discovery
)