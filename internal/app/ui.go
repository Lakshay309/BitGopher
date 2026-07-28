package app

import "github.com/google/uuid"

type UICommandType int

const (
	UIPing UICommandType = iota
	UIPeers
	UIDial
	UIDisconnect
)

type UIResponse struct {
    Payload any
    Err error
}

type UICommand struct {
	Type UICommandType
	PeerID  uuid.UUID
	Payload string
	Response chan UIResponse
}
