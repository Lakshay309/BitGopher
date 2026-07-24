package transport

import (
	"net"
)

type Connection struct {
	Conn net.Conn

	SendChan chan Packet
}

// func (c *Connection) writeLoop() {
//     for packet := range c.SendChan {
//         if err := writePacket(c.Conn, packet); err != nil {
//             slog.Error("[writeLoop]","err",err)
//             return
//         }
//     }
// }
