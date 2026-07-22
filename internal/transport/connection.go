package transport

import (
	"net"

	"github.com/google/uuid"
)

type Connection struct {
	PeerID uuid.UUID
	Conn net.Conn
}
