package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/Lakshay309/bitgopher/internal/app"
)

func main() {
	application, err := app.NewApp()
	if err != nil {
		slog.Error("[main]", "err", err)
	}
	application.Start()
	time.Sleep(20 *time.Second)
	application.DialSomeone()
	time.Sleep(20 * time.Second)
	peers := application.GetPeers()
	for _, peer := range peers {
		fmt.Println(peer.ID, " ", peer.TCPAddr, peer.Conn)
	}
	
	select {}
}
