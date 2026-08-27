package app

import (
	"fmt"
	"strconv"
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
	UIBlackList
	UIGetBlackList

	// file UI commands
	UISearchForAFile
	UIGetSeededFiles
	UIGetSeededFileUsingHash
	UISeedLocalFile
)

type UIResponse struct {
	Payload any
	Err     error
}

type FilePayload struct {
	FileName    string
	Path        string
	FileHash    []byte
	Keywords    []string
	Description string
}

type UICommand struct {
	Type         UICommandType
	RemotePeerID uuid.UUID
	Payload      string
	FilePayload  FilePayload
	Response     chan UIResponse
}

// * handling UI function
func (a *App) handleUIPeersEvent(response chan UIResponse) {
	peers := a.GetPeers()

	rows := make([][]string, 0, len(peers)+1)

	// Header
	rows = append(rows, []string{
		"#",
		"ID",
		"Connected",
		"Last Seen",
		"Last Activity",
	})

	for i, peer := range peers {
		rows = append(rows, []string{
			strconv.Itoa(i + 1),
			peer.ID.String(),
			strconv.FormatBool(peer.Connected),
			peer.LastSeen.Format(time.RFC3339),
			peer.LastActivity.Format(time.RFC3339),
		})
	}

	response <- UIResponse{
		Payload: rows,
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

	if peer.Conn == nil {
		// first dail
		err := a.transport.Connect(peer.TCPAddr)
		if err != nil {
			a.UiLogChan <- UILog{
				Payload:   "handshake error",
				Originate: "App.handleUIPingEvent",
			}
		}
	}
	peer = a.GetPeer(cmd.RemotePeerID)
	if peer == nil || peer.Conn == nil {
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
