package app

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Lakshay309/bitgopher/internal/common"
	"github.com/Lakshay309/bitgopher/internal/fileManager"
)

// TODO: search functionality
// we have to test this
// get file using filename
// TODO: test this
func (a *App) handleSearchForAFile(cmd UICommand) {
	if cmd.Response == nil {
		return
	}
	fileName := cmd.FilePayload.FileName
	fileName = strings.Trim(fileName, " ")
	if len(fileName) == 0 {
		cmd.Response <- UIResponse{
			Err: fmt.Errorf("[SearchForAFile] file name cannot be empty"),
		}
		return
	}

	if a.fileManager.State() != fileManager.StateReady {
		cmd.Response <- UIResponse{
			Err: fmt.Errorf("[SearchForFile] file manager is not ready"),
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), common.ContextTimeInMinute*time.Minute)
	defer cancel()

	resp := make(chan []fileManager.FileInfo, 1)
	select {
	case <-ctx.Done():
		cmd.Response <- UIResponse{
			Err: fmt.Errorf("[SearchForAFile] context cancelled before event queued: %w", ctx.Err()),
		}
		return
	case a.fileManager.FileEventChan <- fileManager.FileEvent{
		Type: fileManager.SearchEvent,
		Metadata: fileManager.ShareMetadata{
			DisplayName: fileName,
		},
		Response: resp,
	}:
	}
	select {
	case <-ctx.Done():
		cmd.Response <- UIResponse{
			Err: fmt.Errorf("[SearchforAfile] timeout"),
		}
		return
	case result := <-resp:
		cmd.Response <- UIResponse{
			Payload: result,
		}
	}
}

// returns the []filemanager.FileInfo
func (a *App) handleGetSeededFiles(cmd UICommand) {
	if cmd.Response == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), common.ContextTimeInMinute*time.Minute)
	defer cancel()

	resp := make(chan []fileManager.FileInfo, 1)

	select {
	case <-ctx.Done():
		sendUIResponse(cmd.Response, UIResponse{Err: ctx.Err()})
		return
	case a.fileManager.FileEventChan <- fileManager.FileEvent{
		Type:     fileManager.GetFilesEvent,
		Response: resp,
	}:
	}

	select {
	case <-ctx.Done():
		sendUIResponse(cmd.Response, UIResponse{Err: ctx.Err()})
	case result := <-resp:
		sendUIResponse(cmd.Response, UIResponse{Payload: result})
	}
}

// TODO: complete this code
// getfile using filehash
func (a *App) handleGetSeededFileUsingHash(cmd UICommand) {
	if cmd.Response == nil {
		return
	}

	hash := cmd.FilePayload.FileHash
	if len(hash) == 0 {
		sendUIResponse(cmd.Response, UIResponse{
			Err: fmt.Errorf("file hash is empty or nil"),
		})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), common.ContextTimeInMinute*time.Minute)
	defer cancel()

	resp := make(chan []fileManager.FileInfo, 1)
	req := fileManager.FileEvent{
		Type:     fileManager.GetFileEvent,
		FileHash: hash,
		Response: resp,
	}

	select {
	case <-ctx.Done():
		sendUIResponse(cmd.Response, UIResponse{Err: ctx.Err()})
		return
	case a.fileManager.FileEventChan <- req:
	}

	select {
	case <-ctx.Done():
		sendUIResponse(cmd.Response, UIResponse{Err: ctx.Err()})
	case result := <-resp:
		sendUIResponse(cmd.Response, UIResponse{Payload: result})
	}
}

func (a *App) handleSeedLocalFile(cmd UICommand) {
	if cmd.Response == nil {
		return
	}

	path := cmd.FilePayload.Path
	description := cmd.FilePayload.Description
	keywords := cmd.FilePayload.Keywords

	if path == "" || len(description) == 0 || len(keywords) == 0 {
		sendUIResponse(cmd.Response, UIResponse{
			Err: fmt.Errorf("invalid arguments: path, description, and keywords are required"),
		})
		return
	}

	info, err := os.Stat(path)
	if err != nil {
		sendUIResponse(cmd.Response, UIResponse{
			Err: fmt.Errorf("file access error: %w", err),
		})
		return
	}
	if info.IsDir() {
		sendUIResponse(cmd.Response, UIResponse{
			Err: fmt.Errorf("path is a directory, expected a file"),
		})
		return
	}

	go func() {
		req := fileManager.SeedRequest{
			Type:        fileManager.LocalSeed,
			Path:        path,
			Description: description,
			Keywords:    keywords,
		}

		select {
		case a.fileManager.SeedChan <- req:
			fmt.Println("seeding request queued successfully")
		default:
			fmt.Println("failed to queue seeding request: manager channel full")
		}
	}()

	sendUIResponse(cmd.Response, UIResponse{
		Payload: "seeding...",
	})
}


func sendUIResponse(ch chan<- UIResponse, resp UIResponse) {
	select {
	case ch <- resp:
	default:
		ch <- UIResponse{
			Payload: "default response",
			Err:     fmt.Errorf("Default response"),
		}
	}
}

/* TODO: what do we have to do for file handling part

*TODO : peer after disconnect gets re connect solve that


* create an api that will can send request to other peer for the search of particular file do it exist with them or not

*if that particular file Exist we have to have send info about the file metadata

* if not exist we still have to response negative to the peer

* we also have to maintain a filetracker that will see who have which file with them and store relivant info about the file in that peer(reciever peer)

* then we will have a option to to get a file using the name that is asociated with it we will take hash from the filetracker and then send that hash to the peer that have that particular file like we wnat that file

* we also have to build relevant protocol for the file transfer how the file trnafer protocol should look

* we also ahve to create some mechanism that will  recieve the file from the peer


* * what we have currently in filemanager

* we have a way to get the file information for a particular hash ( we can get the metadata about the file which can be used to steam the file)
* we have a functionality to search the file using name and stuff in filemanager
* remove a particular file using the fileshash
* get all the file we are seeding locally
* we also have a complete funcitonlity to seed a file using the file path and some metadata fields

 */
