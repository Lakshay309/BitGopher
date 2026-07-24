package transport

import (
	"log/slog"
	"net"
)

// const (
// 	pong = "PONG"
// 	ping = "PING"
// )

func (t *TCPTransport) handlePing(conn net.Conn) {
	slog.Info("received Ping")
	err := writePacket(conn, Packet{
		Type: PongPacket,
	})
	if err != nil {
		slog.Error("[handlePing]", "err", err)
		return
	}
}

func (t *TCPTransport) handlePong() {
	slog.Info("received Pong")
}
