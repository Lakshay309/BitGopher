package fileManager

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

func (fm *FileManager) SearchFile(event FileEvent) {
	if event.Response == nil {
		return
	}

	var res []FileInfo
	seen := make(map[string]bool)

	if len(event.FileHash) > 0 {
		hashKey := hex.EncodeToString(event.FileHash)
		if fileInfo, ok := fm.filesByHash[hashKey]; ok {
			res = fm.appendUniqueFile(res, seen, fileInfo)
		} else if fileInfo, ok := fm.filesByHash[string(event.FileHash)]; ok {
			res = fm.appendUniqueFile(res, seen, fileInfo)
		}

		sendSearchResponse(event.Response, res)
		return
	}

	// 2. Name-Based Search & Tokenization
	name := strings.TrimSpace(event.Metadata.DisplayName)
	if name == "" {
		sendSearchResponse(event.Response, res)
		return
	}

	// Build search variations: exact, lowercase, uppercase, and tokenized terms
	variations := []string{
		name,
		strings.ToLower(name),
		strings.ToUpper(name),
	}
	variations = append(variations, tokenize(name)...)

	// Look up variations in the index and deduplicate entries
	for _, query := range variations {
		if fileInfos, ok := fm.searchIndex[query]; ok {
			for _, fileInfo := range fileInfos {
				res = fm.appendUniqueFile(res, seen, fileInfo)
			}
		}
	}

	// 3. Return deduplicated results safely
	sendSearchResponse(event.Response, res)
}

// Standalone method to append unique files based on path
func (fm *FileManager) appendUniqueFile(res []FileInfo, seen map[string]bool, info *FileInfo) []FileInfo {
	if info != nil && !seen[info.Path] {
		seen[info.Path] = true
		return append(res, *info)
	}
	return res
}

// Standalone non-blocking helper to send response back to the caller channel
func sendSearchResponse(ch chan<- []FileInfo, res []FileInfo) {
	select {
	case ch <- res:
	default:
		slog.Warn("SearchFile response channel full or abandoned")
	}
}

func (fm *FileManager) Get(hash []byte) (*FileInfo, bool) {
	fileInfo, ok := fm.filesByHash[string(hash)]
	if !ok {
		return nil, false
	}
	return fileInfo, ok
}

// TODO: do this first after that work on fileTracker !!!!!!
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
			fm.getFiles(event.Response)

		case GetFileEvent:
			fm.getFile(event.Response, event.FileHash)

		case SearchEvent:
			fm.SearchFile(event)
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

func (fm *FileManager) getFile(resp chan []FileInfo, fileHash []byte) {
	if resp == nil || fileHash == nil {
		return
	}

	fileEntry, ok := fm.filesByHash[string(fileHash)]
	if !ok {
		resp <- []FileInfo{}
		return
	}

	resp <- []FileInfo{*fileEntry}
}

func (fm *FileManager) getFiles(resp chan []FileInfo) {
	buffer := make([]FileInfo, 0, len(fm.filesByHash))

	if resp == nil {
		return
	}

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

	resp <- buffer
}

func (fm *FileManager) setState(state StateType) {
	fm.state.Store(state)
}

func (fm *FileManager) State() StateType {
	return fm.state.Load().(StateType)
}
