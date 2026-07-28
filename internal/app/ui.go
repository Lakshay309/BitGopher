package app

import (
	"fmt"
	"strings"
	"time"

	"github.com/Lakshay309/bitgopher/internal/transport"
	"github.com/google/uuid"
)

type UICommandType int

const (
	UIPing UICommandType = iota
	UIPeers
	UIDial
	UIDisconnect
)

type UIResponse struct {
	Payload any
	Err     error
}

type UICommand struct {
	Type         UICommandType
	RemotePeerID uuid.UUID
	Payload      string
	Response     chan UIResponse
}

// * handling UI function
func (a *App) handleUIPeersEvent() {
	peers := a.GetPeers()

	if len(peers) == 0 {
		a.UiLogChan <- UILog{
			Payload:   "No peers found.",
			Originate: "App.handleUIPeersEvent",
		}
		return
	}

	var builder strings.Builder

	fmt.Fprintf(&builder, "Found %d peer(s):\n\n", len(peers))

	for _, peer := range peers {
		fmt.Fprintf(&builder, "ID: %s\nConnected: %t\nLast Seen: %s\nLast Activity: %s\n\n",
			peer.ID,
			peer.Connected,
			peer.LastSeen.Format(time.RFC3339),
			peer.LastActivity.Format(time.RFC3339))
	}

	a.UiLogChan <- UILog{
		Payload:   builder.String(),
		Originate: "App.handleUIPeersEvent",
	}
}

func (a *App) handleUIPingEvent(cmd UICommand) {
	peer := a.GetPeer(cmd.RemotePeerID)
	if peer == nil {
		a.UiLogChan <- UILog{
			Payload:   fmt.Sprintf("Peer %s not found", cmd.RemotePeerID),
			Originate: "App.handleUIPingEvent",
		}
		return
	}

	if err := a.SendPacket(peer.Conn, cmd.RemotePeerID, transport.PingPacket, nil); err != nil {
		a.UiLogChan <- UILog{
			Payload:   fmt.Sprintf("Failed to send ping to peer %s", cmd.RemotePeerID),
			Error:     err,
			Originate: "App.handleUIPingEvent",
		}
		return
	}
}

func (a *App) handleUIDisconnect(cmd UICommand) {
	peer := a.GetPeer(cmd.RemotePeerID)
	if peer == nil {
		a.UiLogChan <- UILog{
			Payload:   fmt.Sprintf("Peer %s not found.", cmd.RemotePeerID),
			Originate: "App.handleUIDisconnect",
		}
		return
	}

	a.DisconnectPeer(cmd.RemotePeerID)

	a.UiLogChan <- UILog{
		Payload:   fmt.Sprintf("Disconnected peer %s.", cmd.RemotePeerID),
		Originate: "App.handleUIDisconnect",
	}
}
