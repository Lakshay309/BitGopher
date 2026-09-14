package filetracker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Lakshay309/bitgopher/internal/peer"
	"github.com/google/uuid"
)

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

/*
* I think we have to also do create a file meta data file and also start creating the file using the file  liek building protocol here

*/

type FileTracker struct {
	// Map file name to a slice of peer UUIDs that host the file
	fileToPeer map[string][]uuid.UUID

	// Reverse lookup: map peer UUID to a set of files hosted by that peer (for O(1) removal)
	peerToFiles map[uuid.UUID]map[string]struct{}

	FileTrackerChan     chan FileTrackerEvent
	FilePeerManagerChan chan peer.PeerEvent

	quit chan struct{}
	wg   sync.WaitGroup
}

// func (fm *FileTracker)


func NewFileTracker(peerManagerChan chan peer.PeerEvent) *FileTracker {
	return &FileTracker{
		fileToPeer:          make(map[string][]uuid.UUID),
		peerToFiles:         make(map[uuid.UUID]map[string]struct{}),
		FileTrackerChan:     make(chan FileTrackerEvent, FileTrackerChanSize),
		FilePeerManagerChan: peerManagerChan,
		quit:                make(chan struct{}),
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

// AddFile registers a peer mapping for a given file (Helper method)
func (f *FileTracker) AddFile(fileName string, peerID uuid.UUID) {
	// Add to fileToPeer
	peers := f.fileToPeer[fileName]
	for _, id := range peers {
		if id == peerID {
			return // Peer already registered for this file
		}
	}
	f.fileToPeer[fileName] = append(peers, peerID)

	// Add to peerToFiles index
	if _, exists := f.peerToFiles[peerID]; !exists {
		f.peerToFiles[peerID] = make(map[string]struct{})
	}
	f.peerToFiles[peerID][fileName] = struct{}{}
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

		case event, ok := <-f.FilePeerManagerChan:
			if !ok {
				return
			}
			switch event.Type {
			case peer.RemovePeerEvent:
				f.RemovePeerFromFileToPeer(event)
			}
		}
	}
}

// GetPeerWithFile responds with a slice of uuid.UUID containing all peers hosting the file
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

	peers, ok := f.fileToPeer[event.FileName]
	var resp FileTrackerResponse

	if !ok || len(peers) == 0 {
		resp = FileTrackerResponse{
			Err: fmt.Errorf("file peer not found for key: %s", event.FileName),
		}
	} else {
		// Make a copy to avoid race conditions if caller mutates slice
		peersCopy := make([]uuid.UUID, len(peers))
		copy(peersCopy, peers)
		resp = FileTrackerResponse{
			Payload: peersCopy,
		}
	}

	select {
	case <-ctx.Done():
		// Avoid blocking if consumer context timed out
		select {
		case event.Response <- FileTrackerResponse{Err: ctx.Err()}:
		default:
		}
	case event.Response <- resp:
	}
}

// RemovePeerFromFileToPeer removes all entries associated with a disconnected peer
func (f *FileTracker) RemovePeerFromFileToPeer(event peer.PeerEvent) {
	peerID := event.Command.Peer.ID

	files, exists := f.peerToFiles[peerID]
	if !exists {
		return
	}

	// Remove peer from fileToPeer mapping for each file it owned
	for fileName := range files {
		peers := f.fileToPeer[fileName]
		updatedPeers := make([]uuid.UUID, 0, len(peers))

		for _, id := range peers {
			if id != peerID {
				updatedPeers = append(updatedPeers, id)
			}
		}

		if len(updatedPeers) == 0 {
			delete(f.fileToPeer, fileName)
		} else {
			f.fileToPeer[fileName] = updatedPeers
		}
	}

	// Remove peer entry entirely from secondary index
	delete(f.peerToFiles, peerID)
}



/*

0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Magic (0x4654)| Request Type  |   Reserved    |               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+               +
|                       Packet Size (uint64)                    |
|                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                               |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|                     Transfer UUID (16 bytes)                  |
|                                                               |
|                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                               |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|                                                               |
+                     File Hash (32 bytes SHA-256)               +
|                                                               |
|                                                               |
|                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                               |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|                       Chunk Index (uint64)                    |
|                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                               |     Chunk Size (uint32)       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    Chunk Checksum (CRC32 uint32)              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Payload Bytes                         |
|                              ...                              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*/