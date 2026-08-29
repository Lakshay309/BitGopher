package gui

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Lakshay309/bitgopher/internal/app"
	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/Lakshay309/bitgopher/internal/fileManager"
)

func (g *GUI) handleSearchFile(fileName string) {
	if strings.TrimSpace(fileName) == "" {
		slog.Error("Search failed: file name cannot be empty")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), common.ContextTimeInMinute*time.Minute)
	defer cancel()

	resp := make(chan app.UIResponse, 1)

	select {
	case <-ctx.Done():
		slog.Error("Failed to dispatch search command: timed out")
		return
	case g.app.UiChan <- app.UICommand{
		Type: app.UISearchForAFile,
		FilePayload: app.FilePayload{
			FileName: fileName,
		},
		Response: resp,
	}:
	}

	var result app.UIResponse
	select {
	case result = <-resp:
	case <-ctx.Done():
		slog.Error("Failed to search file: command timed out")
		return
	}

	if result.Err != nil {
		slog.Error("Failed to search file", "err", result.Err)
		return
	}

	fileSlice, ok := result.Payload.([]fileManager.FileInfo)
	if !ok {
		slog.Error("Conversion failed: expected []fileManager.FileInfo", "got", fmt.Sprintf("%T", result.Payload))
		return
	}

	headers := []string{"INDEX", "DISPLAY NAME", "KEYWORDS", "DESCRIPTION", "PATH"}
	rows := make([][]string, 0, len(fileSlice))

	if len(fileSlice) == 0 {
		rows = append(rows, []string{"-", "No files found", "-", "-", "-"})
	} else {
		for i, f := range fileSlice {
			keywords := strings.Join(f.Keywords, ", ")
			if keywords == "" {
				keywords = "-"
			}
			desc := f.Description
			if desc == "" {
				desc = "-"
			}
			rows = append(rows, []string{
				fmt.Sprintf("%d", i+1),
				f.DisplayName,
				keywords,
				desc,
				f.Path,
			})
		}
	}

	// Calculate dynamic column widths based on content length
	colWidths := make([]int, len(headers))
	for i, h := range headers {
		colWidths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}

	// Build border lines dynamically matching theme
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

	// Render table header
	fmt.Println()
	fmt.Println(topBorder)
	for i, h := range headers {
		fmt.Printf("│ %-*s ", colWidths[i], h)
	}
	fmt.Println("│")
	fmt.Println(midBorder)

	// Render data rows
	for _, row := range rows {
		for i, cell := range row {
			fmt.Printf("│ %-*s ", colWidths[i], cell)
		}
		fmt.Println("│")
	}

	fmt.Println(botBorder)
	fmt.Println()
}

func (g *GUI) handleLocalSeedFile(args []string) {
	var path, description string
	var keywords []string

	for i := 0; i < len(args); i++ {
		switch strings.ToLower(args[i]) {
		case "path":
			if i+1 < len(args) {
				path = args[i+1]
				i++
			}
		case "description":
			if i+1 < len(args) {
				description = args[i+1]
				i++
			}
		case "keywords":
			for i+1 < len(args) {
				next := strings.ToLower(args[i+1])
				if next == "path" || next == "description" || next == "keywords" {
					break
				}
				keywords = append(keywords, args[i+1])
				i++
			}
		}
	}

	if path == "" || description == "" || len(keywords) == 0 {
		slog.Error("Failed to start seeding: path, description, and keywords are required")
		return
	}

	resp := make(chan app.UIResponse, 1)

	select {
	case g.app.UiChan <- app.UICommand{
		Type: app.UISeedLocalFile,
		FilePayload: app.FilePayload{
			Path:        path,
			Keywords:    keywords,
			Description: description,
		},
		Response: resp,
	}:
	default:
		slog.Error("Failed to start seeding: command queue full")
		return
	}

	// Wait for acknowledgement (success/fail)
	result := <-resp

	if result.Err != nil {
		slog.Error("Failed to start seeding", "err", result.Err)
		return
	}

	slog.Info("Seeding status", "status", result.Payload, "path", path)
}
