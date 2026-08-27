package fileManager

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

type SeedType uint8

const (
	LocalSeed SeedType = iota
	RemoveSeed
	RemoteSeed
	ReSeed
	
)

type SeedRequest struct {
	Type        SeedType
	Path        string
	Description string
	Keywords    []string
	FileInfo    FileInfo

	PeerID uuid.UUID
	// Metadata *FileMetadata
}

/*
localSeed creates a local seed from the file specified in the SeedRequest.

Required from SeedRequest:
  - Path
  - Description
  - Keywords

Calls:
  - populateFileHash
  - populateChunkMetadata
  - writeMetadataFile

Side effects:
  - Creates the chunk-hash file.
  - Creates the metadata file.
  - Sends an AddFileEvent through FileEventChan.

On failure, removes any partially created files.
*/
func (fm *FileManager) localSeed(req SeedRequest) error {
	fileInfo, err := os.Stat(req.Path)
	if err != nil {
		return err
	}

	path, err := filepath.Abs(req.Path)
	if err != nil {
		return err
	}
	// TODO: in version 2 we will be able to pass folders but not today
	if fileInfo.IsDir() || !fileInfo.Mode().IsRegular() {
		return fmt.Errorf("[localSeed]: we can only seed file at current stage!!")
	}

	//  to check if this file is seeded or not we have to check
	_, ok := fm.localSeededFiles[path]
	if ok {
		return fmt.Errorf("file already seeded: %s", path)
	}
	// check if same name file exist or not
	_, err = os.Stat(filepath.Join(fm.sharedDir, MetaDataDir, filepath.Base(path)+MetaDataExtensionType))
	if err == nil {
		return fmt.Errorf("[localSeed] same name file already seeded!!")
	}

	metadata := ShareMetadata{
		Version:     MetadataVersion,
		DisplayName: filepath.Base(path),
		Path:        req.Path,
		Description: req.Description,
		Keywords:    req.Keywords,
		Size:        fileInfo.Size(),
		ModifiedAt:  fileInfo.ModTime().Unix(),
	}

	chunkFilePath := filepath.Join(
		fm.sharedDir,
		ChunkDir,
		metadata.DisplayName+ChunkExtensionType,
	)

	metaFilePath := filepath.Join(
		fm.sharedDir,
		MetaDataDir,
		metadata.DisplayName+MetaDataExtensionType,
	)

	success := false
	defer func() {
		if success {
			return
		}
		slog.Error("[localSeed]","local seed stop here","ERROR")

		_ = os.Remove(chunkFilePath)
		_ = os.Remove(metaFilePath)
	}()

	if err := fm.populateFileHash(path, &metadata); err != nil {
		return err
	}
	
	if err := fm.populateChunkMetadata(req.Path, &metadata); err != nil {
		return err
	}

	if err := fm.writeMetadataFile(metadata); err != nil {
		return err
	}

	fm.FileEventChan <- FileEvent{
		Type:     AddFileEvent,
		Metadata: metadata,
	}
	success = true
	return nil
}


/*
populateFileHash calculates the SHA-256 hash of the file at path and stores
the resulting hash in metadata.FileHash.

Required from metadata:
  - FileHash is updated.
*/
func (fm *FileManager) populateFileHash(path string, metadata *ShareMetadata) error {
	fileHash, err := fm.hashFile(path)
	if err != nil {
		return err
	}

	metadata.FileHash = fileHash

	return nil
}

/*
writeMetadataFile serializes metadata as indented JSON and writes it to the
metadata directory using metadata.DisplayName as the filename.

Required from metadata:
  - DisplayName

Creates:
  - <DisplayName>.bgmeta
*/
func (fm *FileManager) writeMetadataFile(metadata ShareMetadata) error {
	metaPath := filepath.Join(
		fm.sharedDir,
		MetaDataDir,
		metadata.DisplayName+MetaDataExtensionType,
	)

	data, err := json.MarshalIndent(metadata, "", "    ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(metaPath, data, 0644); err != nil {
		os.Remove(metaPath)
	}
	return nil
}

/*
hashFile calculates and returns the SHA-256 hash of the file at path.

Returns:
  - SHA-256 hash as a byte slice.
*/
func (fm *FileManager) hashFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	hasher := sha256.New()

	if _, err := io.Copy(hasher, file); err != nil {
		return nil, err
	}

	return hasher.Sum(nil), nil
}


/* 
removeSeed removes all locally generated files associated with a seed. 
Required fields from 
	FileInfo: 
		- DisplayName  
		- Metadata.FileHash 
	Side effects: 
		- Removes the metadata file. 
		- Removes the chunk file. 
		- Sends a RemoveFileEvent through FileEventChan. 
*/
func (fm *FileManager) removeSeed(file FileInfo) {

	metadataFilePath := filepath.Join(fm.sharedDir, MetaDataDir, file.DisplayName+MetaDataExtensionType)

	chunkFilePath := filepath.Join(fm.sharedDir, ChunkDir, file.DisplayName+ChunkExtensionType)

	if err := os.Remove(metadataFilePath); err != nil && !os.IsNotExist(err) {
		return
	}

	if err := os.Remove(chunkFilePath); err != nil && !os.IsNotExist(err) {
		return
	}

	fm.FileEventChan <- FileEvent{
		Type:     RemoveFileEvent,
		FileHash: file.Metadata.FileHash,
	}
}


func (fm *FileManager) remoteSeed(req SeedRequest) error {
	return nil
}
