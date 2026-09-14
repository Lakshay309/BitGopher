package fileManager

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Lakshay309/bitgopher/internal/common"
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

	// cloasing functionality
	// TODO: work on this (like we did in filetracker module)
	quit chan struct{}
	wg   sync.WaitGroup
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
		quit:             make(chan struct{}),
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
func sendSearchResponse(ch chan<- FileEventResponse, res []FileInfo) {
	select {
	// TODO:
	case ch <- FileEventResponse{
		FileInfos: res,
	}:
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
			fm.getFiles(event)

		case GetFileEvent:
			fm.getFile(event)

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

func (fm *FileManager) getFile(event FileEvent) {

	if event.Response == nil || event.FileHash == nil {
		return
	}

	fileHash := event.FileHash
	resp := event.Response

	fileEntry, ok := fm.filesByHash[string(fileHash)]
	if !ok {
		resp <- FileEventResponse{
			Err: fmt.Errorf("Error: No file with this hash"),
		}
		return
	}

	resp <- FileEventResponse{
		FileInfos: []FileInfo{*fileEntry},
	}
}

func (fm *FileManager) getFiles(event FileEvent) {
	if event.Response == nil {
		return
	}
	resp := event.Response

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

	resp <- FileEventResponse{
		FileInfos: buffer,
	}
}

func (fm *FileManager) setState(state StateType) {
	fm.state.Store(state)
}

func (fm *FileManager) State() StateType {
	return fm.state.Load().(StateType)
}

// TODO: Test this function when possible
func (fm *FileManager) ReadChunk(event FileEvent) {
	// 1. Guard against nil response channels
	if event.Response == nil {
		return
	}

	// 2. Validate basic input requirements
	if event.FileHash == nil && event.Metadata.Path == "" {
		event.Response <- FileEventResponse{
			Err: fmt.Errorf("ERROR: File cannot be reached (missing hash and path)"),
		}
		return
	}

	var path string

	// 3. Resolve path if not explicitly provided
	if event.Metadata.Path == "" {
		resp := make(chan FileEventResponse, 1)
		fm.FileEventChan <- FileEvent{
			Type:     GetFileEvent,
			FileHash: event.FileHash,
			Response: resp,
		}

		result := <-resp
		if result.Err != nil {
			event.Response <- FileEventResponse{Err: result.Err}
			return
		}

		fileInfos := result.FileInfos
		if len(fileInfos) == 0 {
			event.Response <- FileEventResponse{
				Err: fmt.Errorf("ERROR: No file metadata found for the provided hash"),
			}
			return
		}
		path = fileInfos[0].Path
	} else {
		path = event.Metadata.Path
	}

	// 4. Verify file exists and validate bounds
	fileInfo, err := os.Stat(path)
	if err != nil {
		event.Response <- FileEventResponse{
			Err: fmt.Errorf("ERROR: File does not exist or is inaccessible: %w", err),
		}
		return
	}

	offset := event.index * common.ChunkSize
	if offset < 0 || offset >= fileInfo.Size() {
		event.Response <- FileEventResponse{
			Err: fmt.Errorf("ERROR: Chunk index %d out of bounds for file size %d", event.index, fileInfo.Size()),
		}
		return
	}

	// 5. Open file for reading
	file, err := os.Open(path)
	if err != nil {
		event.Response <- FileEventResponse{
			Err: fmt.Errorf("ERROR: Failed to open file: %w", err),
		}
		return
	}
	defer file.Close()

	// 6. Seek to the calculated chunk offset
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		event.Response <- FileEventResponse{
			Err: fmt.Errorf("ERROR: Failed to seek to offset %d: %w", offset, err),
		}
		return
	}

	// 7. Calculate buffer size (accounts for the last partial chunk)
	remainingBytes := fileInfo.Size() - offset
	readSize := min(remainingBytes, int64(common.ChunkSize))

	buffer := make([]byte, readSize)

	// 8. Read the chunk from disk
	n, err := io.ReadFull(file, buffer)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		event.Response <- FileEventResponse{
			Err: fmt.Errorf("ERROR: Failed to read chunk: %w", err),
		}
		return
	}

	// 9. Send response back to caller
	event.Response <- FileEventResponse{
		DataBytes: buffer[:n],
		Err:       nil,
	}
}

func (fm *FileManager) ReadFileChunks(event FileEvent) ([]byte, error) {

	//  Validate basic input requirements
	if event.FileHash == nil && event.Metadata.Path == "" {
		return nil, fmt.Errorf("Filehash and path is nil")
	}

	var path string

	//  Resolve path if not explicitly provided
	if event.Metadata.Path == "" {
		resp := make(chan FileEventResponse, 1)
		fm.FileEventChan <- FileEvent{
			Type:     GetFileEvent,
			FileHash: event.FileHash,
			Response: resp,
		}

		result := <-resp
		if result.Err != nil {
			return nil, result.Err
		}

		fileInfos := result.FileInfos
		if len(fileInfos) == 0 {
			return nil, fmt.Errorf("ERROR: No file metadata found for the provided hash")
		}
		path = fileInfos[0].Path
	} else {
		path = event.Metadata.Path
	}

	// Verify file exists and validate bounds
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("ERROR: File does not exist or is inaccessible: %w", err)
	}

	offset := event.index * common.ChunkSize
	if offset < 0 || offset >= fileInfo.Size() {
		return nil, fmt.Errorf("ERROR: Chunk index %d out of bounds for file size %d", event.index, fileInfo.Size())
	}

	// Open file for reading
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("ERROR: Failed to open file: %w", err)
	}
	defer file.Close()

	// Seek to the calculated chunk offset
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("ERROR: Failed to seek to offset %d: %w", offset, err)
	}

	// Calculate buffer size (accounts for the last partial chunk)
	remainingBytes := fileInfo.Size() - offset
	readSize := min(remainingBytes, int64(common.ChunkSize))

	buffer := make([]byte, readSize)

	// Read the chunk from disk
	n, err := io.ReadFull(file, buffer)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("ERROR: Failed to read chunk: %w", err)
	}
	return buffer[:n], nil
}

func (fm *FileManager) ReadChunkFile(event FileEvent) ([]byte, error) {
	chunkIndex := event.index

	// Construct path to the chunk index file (e.g. sharedDir/chunks/filename.chunk)
	chunkFilePath := filepath.Join(fm.sharedDir, ChunkDir, event.Metadata.DisplayName+ChunkExtensionType)

	// Open the chunk index file for reading
	file, err := os.Open(chunkFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk file at %s: %w", chunkFilePath, err)
	}
	defer file.Close()

	// Each SHA-256 digest is exactly 32 bytes
	const chunkSize = 32
	offset := int64(chunkIndex) * chunkSize

	// Seek to the byte position of the requested chunk index
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to chunk index %d at offset %d: %w", chunkIndex, offset, err)
	}

	// Read exactly 32 bytes for the ith chunk hash
	chunkHash := make([]byte, chunkSize)
	_, err = io.ReadFull(file, chunkHash)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("chunk index %d is out of bounds for file %s", chunkIndex, event.Metadata.DisplayName)
		}
		return nil, fmt.Errorf("failed to read chunk %d: %w", chunkIndex, err)
	}

	return chunkHash, nil
}

const MagicBytes uint16 = 0x4654

func (fm *FileManager) SendFilePacket(event FileEvent) {
	// 1. Read the actual raw payload chunk from the source file
	payload, err := fm.ReadFileChunks(event)
	if err != nil {
		if event.Response != nil {
			event.Response <- FileEventResponse{
				Err: fmt.Errorf("failed to read file chunk: %w", err),
			}
		}
		return
	}

	// 2. Read the 32-byte chunk hash from the .chunk index file
	chunkHash, err := fm.ReadChunkFile(event)
	if err != nil {
		if event.Response != nil {
			event.Response <- FileEventResponse{
				Err: fmt.Errorf("failed to read chunk hash: %w", err),
			}
		}
		return
	}

	// 3. Calculate payload checksum (CRC32)
	payloadChecksum := crc32.ChecksumIEEE(payload)

	// 4. Calculate total packet length
	// Header size: 2(Magic) + 1(Type) + 1(Reserved) + 8(PacketSize) + 16(UUID) + 32(FileHash) + 32(ChunkHash) + 8(Index) + 4(ChunkSize) + 4(CRC32) = 108 bytes
	headerSize := 108
	totalPacketSize := uint64(headerSize + len(payload))

	packet := make([]byte, totalPacketSize)

	// 5. Pack fields in Network Byte Order (Big Endian)
	// [0:2] Magic Bytes (2B)
	binary.BigEndian.PutUint16(packet[0:2], MagicBytes)

	// [2:3] Request Type (1B)
	packet[2] = byte(event.FileProtocol)

	// [3:4] Reserved (1B)
	packet[3] = 0x00

	// [4:12] Total Packet Size (8B)
	binary.BigEndian.PutUint64(packet[4:12], totalPacketSize)

	// [12:28] Peer Transfer UUID (16B)
	copy(packet[12:28], fm.PeerID[:])

	// [28:60] Global File Hash (32B)
	copy(packet[28:60], event.FileHash[:])

	// [60:92] Specific Chunk SHA-256 Hash (32B)
	copy(packet[60:92], chunkHash[:])

	// [92:100] Chunk Index (8B)
	binary.BigEndian.PutUint64(packet[92:100], uint64(event.index))

	// [100:104] Chunk Payload Size (4B)
	binary.BigEndian.PutUint32(packet[100:104], uint32(len(payload)))

	// [104:108] Chunk CRC32 Checksum (4B)
	binary.BigEndian.PutUint32(packet[104:108], payloadChecksum)

	// [108:] Raw Payload Bytes
	copy(packet[108:], payload)

	// 6. Send the encoded binary packet back over the response channel
	if event.Response != nil {
		event.Response <- FileEventResponse{
			DataBytes: packet,
			Err:       nil,
		}
	}
}
