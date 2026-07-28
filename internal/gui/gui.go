package gui

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/Lakshay309/bitgopher/internal/app"
	"github.com/Lakshay309/bitgopher/internal/common"
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

	case "clear":
		fmt.Print("\033[2J\033[3J\033[H")

	case "exit":
		os.Exit(0)

	default:
		fmt.Printf("Unknown command: %s\n", command)
		fmt.Println("Type 'help' to see available commands.")
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