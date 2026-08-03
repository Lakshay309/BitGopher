package filemanager

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

const (
	MaxMetadataSize       = 10 * 1024 * 1024 // 10 Mb
	MetaDataExtensionType = ".bgmeta"
	ChunkExtensionType    = ".bgchunk"
	ChunkDir              = "chunks"
	MetaDataDir           = "metadata"
	MetadataVersion       = 1
)

type ShareMetadata struct {
	Version uint8

	DisplayName string
	Description string
	Keywords    []string

	Path string
	// filename can be dereived from path ok

	Size       int64
	ModifiedAt int64

	FileHash            []byte
	ChunkFileSize       int64
	ChunkFileModifiedAt int64
	ChunkFileHash       []byte

	// Path to the chunk metaData
	ChunkFile string
}

// load only when a transfer starts.
type ChunkMetadata struct {
	ChunkSize   uint32
	TotalChunks uint32
	ChunkHashes [][]byte
}

type FileMetadata struct {
	FileHash []byte
	Size     int64

	Filename string

	ChunkFile     string
	ChunkFileHash []byte
}

type FileInfo struct {
	Metadata FileMetadata

	DisplayName string
	Keywords    []string
	Description string

	Path string
}

type FileManager struct {
	sharedDir string
	PeerID    uuid.UUID

	SeedChan chan SeedRequest

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
		SeedChan:         make(chan SeedRequest),
		searchIndex:      map[string][]*FileInfo{},
		filesByHash:      make(map[string]*FileInfo),
	}

	if err := fm.initialize(); err != nil {
		return nil, err
	}

	return fm, nil
}

func (fm *FileManager) Run() {

	for req := range fm.SeedChan {
		switch req.Type {
		case LocalSeed:
			fm.localSeed(req)
		case RemoteSeed:
			fm.remoteSeed(req)
		}
	}
}

func (fm *FileManager) initialize() error {
	// 1. Ensure shared directory exists.
	if err := os.MkdirAll(fm.sharedDir, 0755); err != nil {
		return fmt.Errorf("Failed to create shared directory: %w", err)
	}
	// scan everyfile indide the shared directory
	if err := fm.loadMetaData(); err != nil {
		return fmt.Errorf("failed to scan shared Directory: %w", err)
	}
	return nil
}

func (fm *FileManager) loadMetaData() error {
	entries, err := os.ReadDir(filepath.Join(fm.sharedDir, MetaDataDir))
	if err != nil {
		return err
	}
	for _, entry := range entries {

		if entry.IsDir() || filepath.Ext(entry.Name()) != MetaDataExtensionType {
			continue
		}

		if err := fm.loadMetaDataFile(filepath.Join(fm.sharedDir, MetaDataDir, entry.Name())); err != nil {
			slog.Error(
				"[FileManager-Scan]",
				"MetaData",
				entry.Name(),
				"err", err,
			)
		}
	}
	return nil
}

func (fm *FileManager) loadMetaDataFile(file string) error {
	// validate metadatfile itself
	info, err := os.Stat(file)
	if err != nil {
		return err
	}

	if !info.Mode().IsRegular() {
		return fmt.Errorf("not a reqular file")
	}
	if info.Size() > MaxMetadataSize {
		return fmt.Errorf("metadata file too large (%d byte)", info.Size())
	}

	// read metadata
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}

	var meta ShareMetadata

	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("invalid metadata: %w", err)
	}

	//  verify original file exists
	fileInfo, err := os.Stat(meta.Path)
	if err != nil {
		return fmt.Errorf("shared file not found: %w", err)
	}

	// Verify file hasn't changed
	if fileInfo.Size() != meta.Size {
		return fmt.Errorf("file size mismatch")
	}

	if fileInfo.ModTime().Unix() != meta.ModifiedAt {
		return fmt.Errorf("file modified after metadata creation")
	}

	// verify chunk metadata exists
	chunkPath := filepath.Join(fm.sharedDir, ChunkDir, meta.ChunkFile)

	chunkInfo, err := os.Stat(chunkPath)
	if err != nil {
		return fmt.Errorf("chunk metadata not found: %w", err)
	}

	if !chunkInfo.Mode().IsRegular() {
		return fmt.Errorf("chunk metadata is not a regular file")
	}

	if chunkInfo.Size() != meta.ChunkFileSize {
		return fmt.Errorf("chunk metadata size mismatch")
	}

	if chunkInfo.ModTime().Unix() != meta.ChunkFileModifiedAt {
		return fmt.Errorf("chunk metadata modified after creation")
	}

	fi := &FileInfo{
		Metadata: FileMetadata{
			FileHash:      meta.FileHash,
			Size:          meta.Size,
			Filename:      filepath.Base(meta.Path),
			ChunkFile:     meta.ChunkFile,
			ChunkFileHash: meta.ChunkFileHash,
		},

		DisplayName: meta.DisplayName,
		Description: meta.Description,
		Keywords:    meta.Keywords,

		Path: meta.Path,
	}

	fm.registerFile(fi)

	return nil
}

func (fm *FileManager) registerFile(file *FileInfo) {
	hash := hex.EncodeToString(file.Metadata.FileHash)

	fm.filesByHash[hash] = file

	fm.localSeededFiles[file.Path] = struct{}{}

	fm.addSearchTerm(file.DisplayName, file)
	fm.addSearchTerm(file.Metadata.Filename, file)
	for _, keyword := range file.Keywords {
		fm.addSearchTerm(keyword, file)
	}
}

func (fm *FileManager) addSearchTerm(term string, file *FileInfo) {
	term = normalize(term)
	if term == "" {
		return
	}
	fm.searchIndex[term] = append(fm.searchIndex[term], file)
}

func (fm *FileManager) Search(query string) []*FileInfo

func (fm *FileManager) Get(hash []byte) (*FileInfo, bool)

func (fm *FileManager) ReadChunk(hash []byte, index uint32) ([]byte, error)

func normalize(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	return strings.Join(strings.Fields(s), " ")
}
