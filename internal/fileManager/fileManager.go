package filemanager

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/google/uuid"
)

type StateType string

const (
	StateInitializing StateType = "initializing"
	StateReloading    StateType = "reloading"
	StateReady        StateType = "ready"
	StateUpdating     StateType = "update"
)

type FileManager struct {
	// this should be absolute path
	sharedDir string
	PeerID    uuid.UUID

	state atomic.Value

	SeedChan chan SeedRequest

	FileEventChan chan FileEvent

	// hash -> file
	filesByHash map[string]*FileInfo // key = hex(FileHash)
	// noramalized search term ->files
	searchIndex map[string][]*FileInfo
	// is file currently seeded for local?
	localSeededFiles map[string]struct{}
}

func NewFileManager(sharedDir string, peerID uuid.UUID) (*FileManager, error) {
	fm := &FileManager{
		sharedDir:        sharedDir,
		PeerID:           peerID,
		localSeededFiles: map[string]struct{}{},
		searchIndex:      map[string][]*FileInfo{},
		filesByHash:      make(map[string]*FileInfo),
		SeedChan:         make(chan SeedRequest, 100),
		FileEventChan:    make(chan FileEvent, 100),
	}
	fm.state.Store(StateInitializing)
	return fm, nil
}

func (fm *FileManager) Run() {
	go fm.seedLoop()
	go fm.fileEventLoop()
}

func (fm *FileManager) Initialize() error {
	// creating main share folder
	if err := os.MkdirAll(fm.sharedDir, 0755); err != nil {
		return fmt.Errorf("failed to create shared directory: %w", err)
	}
	// creating the chunkDir
	if err := os.MkdirAll(filepath.Join(fm.sharedDir, ChunkDir), 0755); err != nil {
		return fmt.Errorf("failed to create shared directory: %w", err)
	}
	// creating the MetadataDir
	if err := os.MkdirAll(filepath.Join(fm.sharedDir, MetaDataDir), 0755); err != nil {
		return fmt.Errorf("failed to create shared directory: %w", err)
	}

	if err := fm.loadMetaData(); err != nil {
		return fmt.Errorf("failed to scan shared directory: %w", err)
	}

	fm.Run()

	fm.setState(StateReady)

	return nil
}

// func (fm *FileManager) Search(query string) []*FileInfo

// func (fm *FileManager) Get(hash []byte) (*FileInfo, bool)

// func (fm *FileManager) ReadChunk(hash []byte, index uint32) ([]byte, error)

func (fm *FileManager) seedLoop() {
	for req := range fm.SeedChan {
		switch req.Type {
		case LocalSeed:
			go fm.localSeed(req)
		case RemoveSeed:
			fm.removeSeed(req.FileInfo)
		case RemoteSeed:
			fm.remoteSeed(req)
		case ReSeed:
			fm.removeSeed(req.FileInfo)
			go fm.localSeed(req)
		}
	}
}

func (fm *FileManager) fileEventLoop() {
	for event := range fm.FileEventChan {
		switch event.Type {
		case AddFileEvent:
			fm.addToMap(event.Metadata)
		case RemoveFileEvent:
			fm.removeFormMap(event.FileHash)
		case GetFilesEvent:
			event.Response <- fm.getFiles()

		}
	}
}

func (fm *FileManager) addToMap(metadata ShareMetadata) {
	fm.setState(StateUpdating)
	defer fm.setState(StateReady)

	file := &FileInfo{
		Metadata: FileMetadata{
			FileHash:      metadata.FileHash,
			Size:          metadata.Size,
			Filename:      filepath.Base(metadata.Path),
			ChunkFile:     metadata.ChunkFile,
			ChunkFileHash: metadata.ChunkFileHash,
		},
		DisplayName: metadata.DisplayName,
		Description: metadata.Description,
		Keywords:    metadata.Keywords,
		Path:        metadata.Path,
	}

	fm.registerFile(file)
}

func (fm *FileManager) removeSearchTerm(term string, file *FileInfo) {
	term = normalize(term)
	if term == "" {
		return
	}

	// Remove the complete normalized term.
	if files, ok := fm.searchIndex[term]; ok {
		files = removeFileFromSlice(files, file)
		if len(files) == 0 {
			delete(fm.searchIndex, term)
		} else {
			fm.searchIndex[term] = files
		}
	}

	// Remove tokenized terms.
	tokens := tokenize(term)
	for _, token := range tokens {
		if token == term {
			continue
		}

		files, ok := fm.searchIndex[token]
		if !ok {
			continue
		}

		files = removeFileFromSlice(files, file)
		if len(files) == 0 {
			delete(fm.searchIndex, token)
		} else {
			fm.searchIndex[token] = files
		}
	}
}


func (fm *FileManager) removeFormMap(fileHash []byte) {
	fm.setState(StateUpdating)
	defer fm.setState(StateReady)

	hash := hex.EncodeToString(fileHash)

	file, ok := fm.filesByHash[hash]
	if !ok {
		return
	}

	// Remove from hash map.
	delete(fm.filesByHash, hash)

	// Remove from local seeded files.
	delete(fm.localSeededFiles, file.Path)

	// Remove from search index.
	fm.removeSearchTerm(file.DisplayName, file)
	fm.removeSearchTerm(file.Metadata.Filename, file)

	for _, keyword := range file.Keywords {
		fm.removeSearchTerm(keyword, file)
	}
}

func (fm *FileManager) getFiles() []FileInfo {
	buffer := make([]FileInfo, 0, len(fm.filesByHash))

	for _, entry := range fm.filesByHash {
		file := FileInfo{
			Metadata: FileMetadata{
				FileHash:      append([]byte(nil), entry.Metadata.FileHash...),
				Size:          entry.Metadata.Size,
				Filename:      entry.Metadata.Filename,
				ChunkFile:     entry.Metadata.ChunkFile,
				ChunkFileHash: append([]byte(nil), entry.Metadata.ChunkFileHash...),
			},
			DisplayName: entry.DisplayName,
			Description: entry.Description,
			Keywords:    append([]string(nil), entry.Keywords...),
			Path:        entry.Path,
		}

		buffer = append(buffer, file)
	}

	return buffer
}

func (fm *FileManager) setState(state StateType) {
	fm.state.Store(state)
}

func (fm *FileManager) State() StateType {
	return fm.state.Load().(StateType)
}