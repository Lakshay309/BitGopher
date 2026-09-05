package filetracker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

/*
what we want to do:
  - when current user generate a request for a file we should be able to store the state of the request
  - what should we have for one request - first thing we should have the peer who have the file ( there can be multiple of peer that have that particluar file ) we will store a list of object that contain
  - peerid (peer that has a particular file),
    that about i think we should first store in file tracker
    other thing we can store are keywords
  - this should also have states it should save thing like how the file will be downloaded
  - also have functionality like resume and stop download how much we have downlaoded a file download a particlular chunk of a file
*/
type FileTrackerType int

const FileTrackerChanSize = 32
const ContextTimeout = 1 * time.Minute

const (
	GetPeerWithFile FileTrackerType = iota
)

type FileTrackerEvent struct {
	Type     FileTrackerType
	FileName string
	Response chan FileTrackerResponse
	Ctx      context.Context
}

type FileTrackerResponse struct {
	Payload any
	Err     error
}

type FileTracker struct {
	// TODO: this should be an array also, also there should be a system that will remove the peers that are disconnected
	fileToPeer      map[string]uuid.UUID
	FileTrackerChan chan FileTrackerEvent
	quit            chan struct{}
	wg              sync.WaitGroup
}

func NewFileTracker() *FileTracker {
	return &FileTracker{
		fileToPeer:      map[string]uuid.UUID{},
		FileTrackerChan: make(chan FileTrackerEvent, FileTrackerChanSize),
		quit:            make(chan struct{}),
	}
}

func (f *FileTracker) Start() {
	f.wg.Add(1)
	go f.Run()
}

func (f *FileTracker) Stop() {
	close(f.quit)
	f.wg.Wait()
}

// AddFile registers a peer mapping (Helper method)
func (f *FileTracker) AddFile(fileName string, peerID uuid.UUID) {
	f.fileToPeer[fileName] = peerID
}

func (f *FileTracker) Run() {
	defer f.wg.Done()
	for {
		select {
		case <-f.quit:
			return
		case event, ok := <-f.FileTrackerChan:
			if !ok {
				return
			}
			switch event.Type {
			case GetPeerWithFile:
				f.GetPeerWithFile(event)
			}
		}
	}
}

func (f *FileTracker) GetPeerWithFile(event FileTrackerEvent) {
	if event.Response == nil {
		return
	}

	baseCtx := event.Ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	ctx, cancel := context.WithTimeout(baseCtx, ContextTimeout)
	defer cancel()

	peerID, ok := f.fileToPeer[event.FileName]
	var resp FileTrackerResponse
	if !ok {
		resp = FileTrackerResponse{
			Err: fmt.Errorf("file peer not found for key: %s", event.FileName),
		}
	} else {
		resp = FileTrackerResponse{
			Payload: peerID,
		}
	}

	select {
	case <-ctx.Done():
		event.Response <- FileTrackerResponse{
			Err: ctx.Err(),
		}
		return
	case event.Response <- resp:
		event.Response <- FileTrackerResponse{
			Payload: peerID,
		}
	}
}
