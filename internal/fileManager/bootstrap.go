package filemanager

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

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

	// Index the complete normalized term.
	fm.searchIndex[term] = append(fm.searchIndex[term], file)

	// Index individual tokens.
	for _, token := range tokenize(term) {
		if token == term {
			continue
		}
		fm.searchIndex[token] = append(fm.searchIndex[token], file)
	}
}
