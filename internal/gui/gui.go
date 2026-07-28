package gui

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/Lakshay309/bitgopher/internal/app"
	"github.com/Lakshay309/bitgopher/internal/common"
)

// ANSI Color Codes
const (
	Reset      = "\033[0m"
	Bold       = "\033[1m"
	Dim        = "\033[2m"
	Cyan       = "\033[36m"
	BrightCyan = "\033[96m"
	Green      = "\033[32m"
	Yellow     = "\033[33m"
	Gray       = "\033[90m"
)

type GUI struct {
	app *app.App
}

func NewGUI() *GUI {
	return &GUI{}
}

func printBanner() {
	// Crisp cyan gradient look for the banner
	banner := `
  ██████╗ ██╗████████╗██████╗  ██████╗  ██████╗ ██╗  ██╗███████╗██████╗ 
  ██╔══██╗██║╚══██╔══╝██╔════╝ ██╔═══██╗██╔══██╗██║  ██║██╔════╝██╔══██╗
  ██████╔╝██║   ██║   ██║  ███╗██║   ██║██████╔╝███████║█████╗  ██████╔╝
  ██╔══██╗██║   ██║   ██║   ██║██║   ██║██╔═══╝ ██╔══██║██╔══╝  ██╔══██╗
  ██████╔╝██║   ██║   ╚██████╔╝╚██████╔╝██║     ██║  ██║███████╗██║  ██║
  ╚═════╝ ╚═╝   ╚═╝    ╚═════╝  ╚═════╝  ╚═╝     ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝`

	fmt.Println(BrightCyan + banner + Reset)
}

func (g *GUI) Start() error {
	reader := bufio.NewReader(os.Stdin)

	var mode common.DiscoveryMode

	for {
		fmt.Println("\nSelect Discovery Mode:")
		fmt.Println("  local (1)")
		fmt.Println("  lan   (2)")
		fmt.Println("  wan   (3) [Not implemented]")
		fmt.Print("> ")

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
			fmt.Println("WAN discovery is not implemented yet.")
			continue

		default:
			fmt.Println("Invalid discovery mode. Please enter 'local', 'lan', or 'wan'.")
		}
	}

password:
	fmt.Print("Password: ")

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
		// Custom stylized prompt indicator
		fmt.Printf(" %sbitgopher%s %s❯%s ", BrightCyan, Reset, Green, Reset)

		line, err := reader.ReadString('\n')
		if err != nil {
			continue
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		switch line {
		case "help":
			printHelp()
			continue
		case "clear":
			// ANSI escape code to clear terminal screen and position cursor top-left
			// \033[2J  - Clear whole screen
			// \033[3J  - Clear scrollback buffer (prevents artifact ghosting)
			// \033[H   - Move cursor to row 1, col 1 (top-left)
			fmt.Print("\033[2J\033[3J\033[H")
			continue
		case "exit":
			fmt.Println("\nExiting BitGopher... Goodbye!")
			return
		}

		g.handleCommand(line)
	}
}

func (g *GUI) handleCommand(cmd string) {

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

func printHelp() {
	fmt.Println()
	fmt.Printf("  %s%sBitGopher Command Reference%s\n", Bold, BrightCyan, Reset)
	fmt.Printf("  %s%s%s\n\n", Gray, strings.Repeat("─", 50), Reset)

	commands := []struct {
		cmd  string
		desc string
	}{
		{"help", "Show this help menu"},
		{"peers", "List all discovered network peers"},
		{"ping <peer-id>", "Send a latency ping to a peer"},
		{"disconnect <peer-id>", "Disconnect from a specific peer"},
		{"clear", "Clear the terminal screen"},
		{"exit", "Gracefully terminate BitGopher"},
	}

	for _, c := range commands {
		fmt.Printf("  %s%-22s%s %s%s%s\n", Green, c.cmd, Reset, Gray, c.desc, Reset)
	}
	fmt.Println()
}