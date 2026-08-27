package gui

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/Lakshay309/bitgopher/internal/app"
	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/Lakshay309/bitgopher/internal/peer"
	"github.com/google/uuid"
)



type GUI struct {
	app *app.App
}

func NewGUI() *GUI {
	return &GUI{}
}

func (g *GUI) Start() error {
	reader := bufio.NewReader(os.Stdin)

	var mode common.DiscoveryMode
	printBanner()

	// Styled Section Header & Options List
	fmt.Printf("\n  %s%sSelect Discovery Mode%s\n", Bold, BrightCyan, Reset)
	fmt.Printf("  %s%s%s\n", Gray, strings.Repeat("─", 30), Reset)
	fmt.Printf("  %s[%s1%s]%s %-8s %s(Local peer discovery only)%s\n", Gray, Green, Gray, Reset, "local", Dim, Reset)
	fmt.Printf("  %s[%s2%s]%s %-8s %s(Discover peers on local network)%s\n", Gray, Green, Gray, Reset, "lan", Dim, Reset)
	fmt.Printf("  %s[%s3%s]%s %-8s %s[Not implemented]%s\n", Gray, Yellow, Gray, Reset, "wan", Dim, Reset)
	fmt.Println()

	for {
		fmt.Printf(" %sbitgopher%s %s(%sselect mode%s) ❯%s ", BrightCyan, Reset, Gray, Yellow, Reset, Green)

		input, err := reader.ReadString('\n')
		if err != nil {
			return err
		}

		input = strings.ToLower(strings.TrimSpace(input))

		switch input {
		case "1", "local":
			mode = common.Local
			goto password

		case "2", "lan":
			mode = common.LAN
			goto password

		case "3", "wan":
			fmt.Printf("  %s▲ WAN discovery is not implemented yet.%s\n\n", Yellow, Reset)
			continue

		default:
			fmt.Printf("  %s✖ Invalid mode. Enter '1', '2', 'local', or 'lan'.%s\n\n", Yellow, Reset)
		}
	}

password:
	fmt.Println()
	fmt.Printf(" %sbitgopher%s %s(%spassword%s) ❯%s ", BrightCyan, Reset, Gray, Yellow, Reset, Green)

	password, err := reader.ReadString('\n')
	if err != nil {
		return err
	}

	password = strings.TrimSpace(password)

	a, err := app.NewApp(mode, password)
	if err != nil {
		return err
	}

	g.app = a

	if err := g.app.Start(); err != nil {
		return err
	}
	fmt.Print("\033[2J\033[3J\033[H")

	g.run()
	return nil
}


func banner_and_info(){
	printBanner()

	// Subtitle & Status bar
	fmt.Printf("  %s%sv1.0.0%s %s│%s P2P Network Node CLI\n", Bold, BrightCyan, Reset, Gray, Reset)
	fmt.Printf("  %sReady! Type %shelp%s %sto view available commands.%s\n\n", Gray, Yellow, Gray, Gray, Reset)
}


func (g *GUI) run() {
	go g.logLoop()

	printBanner()

	// Subtitle & Status bar
	fmt.Printf("  %s%sv1.0.0%s %s│%s P2P Network Node CLI\n", Bold, BrightCyan, Reset, Gray, Reset)
	fmt.Printf("  %sReady! Type %shelp%s %sto view available commands.%s\n\n", Gray, Yellow, Gray, Gray, Reset)

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf(" %sbitgopher%s %s❯%s ", BrightCyan, Reset, Green, Reset)

		line, err := reader.ReadString('\n')
		if err != nil {
			continue
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		g.handleCommand(line)
	}
}
// TODO: when deleting the peer from the will again discovered so we have to create somekind of blacklist to solve that problem as well ok 

func (g *GUI) handleCommand(cmd string) {
	fields := strings.Fields(cmd)
	if len(fields) == 0 {
		return
	}
	command := strings.ToLower(fields[0])
	args := fields[1:]
	switch command {
	case "help":
		printHelp()

	case "peers":
		// peers takes no arguments
		g.handlePeers()

	case "ping":
		// args should contain exactly one peer ID
		g.handlePing(args[0])


	case "disconnect":
		// args should contain exactly one peer ID
		g.handleDisconnect(args[0])

	case "clear":
		fmt.Print("\033[2J\033[3J\033[H")
		banner_and_info()

	case "exit":
		os.Exit(0)
	case "blacklist":
		g.handleBlacklist(args[0])

	case "get_blacklist":
		g.handleGetBlacklist()

	default:
		fmt.Printf("Unknown command: %s\n", command)
		fmt.Println("Type 'help' to see available commands.")
	}
}

// * FIXME: test this if this is right!!???
func (g *GUI) handleGetBlacklist() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp := make(chan app.UIResponse, 1)

	g.app.UiChan <- app.UICommand{
		Type:     app.UIGetBlackList,
		Response: resp,
	}

	var result app.UIResponse
	select {
	case result = <-resp:
	case <-ctx.Done():
		slog.Error("Failed to fetch blacklist: command timed out")
		return
	}

	// 2. Safely check for errors returned by the app layer
	if result.Err != nil {
		slog.Error("Failed to fetch blacklist", "err", result.Err)
		return
	}

	peerSlice, ok := result.Payload.([]peer.PeerInfo)
	if !ok {
		slog.Error("Conversion failed: expected []peer.PeerInfo", "got", fmt.Sprintf("%T", result.Payload))
		return
	}

	// Prepare data rows
	headers := []string{"INDEX", "PEER ID"}
	rows := make([][]string, 0, len(peerSlice))

	if len(peerSlice) == 0 {
		rows = append(rows, []string{"-", "No blacklisted peers"})
	} else {
		for i, p := range peerSlice {
			rows = append(rows, []string{fmt.Sprintf("%d", i+1), fmt.Sprintf("%s", p.ID)})
		}
	}

	// Calculate dynamic column widths based on content
	colWidths := []int{len(headers[0]), len(headers[1])}
	for _, row := range rows {
		if len(row[0]) > colWidths[0] {
			colWidths[0] = len(row[0])
		}
		if len(row[1]) > colWidths[1] {
			colWidths[1] = len(row[1])
		}
	}

	// Build border lines
	var topBorder, midBorder, botBorder string
	for i, w := range colWidths {
		line := strings.Repeat("─", w+2)
		if i == 0 {
			topBorder += "┌" + line
			midBorder += "├" + line
			botBorder += "└" + line
		} else {
			topBorder += "┬" + line
			midBorder += "┼" + line
			botBorder += "┴" + line
		}
	}
	topBorder += "┐"
	midBorder += "┤"
	botBorder += "┘"

	// Render table
	fmt.Println()
	fmt.Println(topBorder)
	fmt.Printf("│ %-*s │ %-*s │\n", colWidths[0], headers[0], colWidths[1], headers[1])
	fmt.Println(midBorder)

	for _, row := range rows {
		fmt.Printf("│ %-*s │ %-*s │\n", colWidths[0], row[0], colWidths[1], row[1])
	}

	fmt.Println(botBorder)
	fmt.Println()
}

func (g *GUI) handleBlacklist(uid string){
	g.app.UiChan<-app.UICommand{
		Type: app.UIBlackList,
		RemotePeerID: uuid.MustParse(uid),
	}
}

func (g *GUI) logLoop() {
	for log := range g.app.UiLogChan {
		if log.Error != nil {
			fmt.Printf("[%-20s] %s (%v)\n",
				log.Originate,
				log.Payload,
				log.Error,
			)
		} else {
			fmt.Printf("[%-20s] %s\n",
				log.Originate,
				log.Payload,
			)
		}
	}
}

func (g *GUI)handlePing(peerIdString string){
	peerId ,err:=uuid.Parse(peerIdString)
	if err!=nil{
		slog.Error("[handlePing]","err",err)
	}
	g.app.UiChan<-app.UICommand{
		Type: app.UIPing,
		RemotePeerID: peerId,
	}
}


func (g *GUI) handleDisconnect(peerIdString string){
	peerId ,err:=uuid.Parse(peerIdString)
	if err!=nil{
		slog.Error("[handlePing]","err",err)
	}
	g.app.DisconnectPeer(peerId)
}