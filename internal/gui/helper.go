package gui

import (
	"fmt"
	"strings"

	"github.com/Lakshay309/bitgopher/internal/app"
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

func printBanner() {
	// Crisp cyan gradient look for the banner
	banner := `
  ██████╗ ██╗████████╗██████╗  ██████╗  ██████╗ ██╗  ██╗███████╗██████╗ 
  ██╔══██╗██║╚══██╔══╝██╔════╝ ██╔═══██╗██╔══██╗██║  ██║██╔════╝██╔══██╗
  ██████╔╝██║   ██║   ██║  ███╗██║   ██║██████╔╝███████║█████╗  ██████╔╝
  ██╔══██╗██║   ██║   ██║   ██║██║   ██║██╔═══╝ ██╔══██║██╔══╝  ██╔══██╗
  ██████╔╝██║   ██║   ╚██████╔╝╚██████╔╝██║     ██║  ██║███████╗██║  ██║
  ╚═════╝ ╚═╝   ╚═╝    ╚═════╝  ╚═════╝ ╚═╝     ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝`

	fmt.Println(BrightCyan + banner + Reset)
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

func (g *GUI) handlePeers() {
	resp := make(chan app.UIResponse, 1)
	g.app.UiChan <- app.UICommand{
		Type:     app.UIPeers,
		Response: resp,
	}
	result := <-resp
	if tableData, ok := result.Payload.([][]string); ok {
		printPeersTable(tableData)
	} else {
		fmt.Printf(" %sFailed to parse peers response.%s\n", Yellow, Reset)
	}
}

func printPeersTable(payload [][]string) {
	if len(payload) == 0 {
		fmt.Printf("  %sNo peer data available.%s\n\n", Yellow, Reset)
		return
	}

	headers := payload[0]
	rows := payload[1:]

	// 1. Calculate maximum width needed for each column
	colWidths := make([]int, len(headers))
	for i, h := range headers {
		colWidths[i] = len(h)
	}

	for _, row := range rows {
		for i, cell := range row {
			if i < len(colWidths) && len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}

	// 2. Build border divider lines
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

	// 3. Print Header
	fmt.Println()
	fmt.Printf("  %s%s%s\n", Gray, topBorder, Reset)
	fmt.Printf("  %s│%s", Gray, Reset)
	for i, h := range headers {
		fmt.Printf(" %s%-*s%s %s│%s", Bold+BrightCyan, colWidths[i], h, Reset, Gray, Reset)
	}
	fmt.Println()
	fmt.Printf("  %s%s%s\n", Gray, midBorder, Reset)

	// 4. Print Rows with status highlights
	for _, row := range rows {
		fmt.Printf("  %s│%s", Gray, Reset)
		for i, cell := range row {
			if i >= len(colWidths) {
				continue
			}

			// Color-code the "Connected" column
			if headers[i] == "Connected" {
				if cell == "true" {
					fmt.Printf(" %s%-*s%s %s│%s", Green, colWidths[i], cell, Reset, Gray, Reset)
				} else {
					fmt.Printf(" %s%-*s%s %s│%s", Yellow, colWidths[i], cell, Reset, Gray, Reset)
				}
			} else {
				fmt.Printf(" %-*s %s│%s", colWidths[i], cell, Gray, Reset)
			}
		}
		fmt.Println()
	}

	fmt.Printf("  %s%s%s\n\n", Gray, botBorder, Reset)
}
