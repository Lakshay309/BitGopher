package filemanager

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

type SeedType uint8

const (
	LocalSeed SeedType = iota
	RemoteSeed
	ChunkSize = 1 << 20
)

type SeedRequest struct {
	Type        SeedType
	Path        string
	Description string
	Keywords    []string

	PeerID   uuid.UUID
	Metadata *FileMetadata
}

// TODO: we have to add the new file the we just created to the actuall filemanager object maps!!
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

	// now we will generate the FileMetadata Info
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

	success = true
	return nil
}

func (fm *FileManager) populateFileHash(path string, metadata *ShareMetadata) error {
	fileHash, err := fm.hashFile(path)
	if err != nil {
		return err
	}

	metadata.FileHash = fileHash
	return nil
}

func (fm *FileManager) populateChunkMetadata(srcPath string, metadata *ShareMetadata) error {
	chunkFilePath := filepath.Join(
		fm.sharedDir,
		ChunkDir,
		metadata.DisplayName+ChunkExtensionType,
	)

	if err := fm.createChunkFile(srcPath, chunkFilePath); err != nil {
		return err
	}

	metadata.ChunkFile = metadata.DisplayName + ChunkExtensionType

	chunkFileInfo, err := os.Stat(chunkFilePath)
	if err != nil {
		return err
	}

	metadata.ChunkFileSize = chunkFileInfo.Size()
	metadata.ChunkFileModifiedAt = chunkFileInfo.ModTime().Unix()

	chunkFileHash, err := fm.hashFile(chunkFilePath)
	if err != nil {
		return err
	}

	metadata.ChunkFileHash = chunkFileHash

	return nil
}

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
	if err:=os.WriteFile(metaPath, data, 0644);err!=nil{
		os.Remove(metaPath)
	}
	return nil
}

func (fm *FileManager) createChunkFile(srcPath string, dstPath string) error {
	buffer := make([]byte, ChunkSize)

	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	success := false

	defer func() {
		dstFile.Close()

		if !success {
			os.Remove(dstPath)
		}
	}()

	for {
		n, err := srcFile.Read(buffer)

		if n > 0 {
			digest := sha256.Sum256(buffer[:n])

			written, err := dstFile.Write(digest[:])
			if err != nil {
				return err
			}

			if written != len(digest) {
				return io.ErrShortWrite
			}
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}
	}

	success = true
	return nil
}

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

func (fm *FileManager) remoteSeed(req SeedRequest) error {
	return nil
}
