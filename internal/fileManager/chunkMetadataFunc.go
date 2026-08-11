package filemanager

import (
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
)
/*
populateChunkMetadata creates the chunk file for srcPath and populates
the chunk-related fields of metadata.

Required from metadata:
	- srcPath
    - DisplayName

Updates:
   - ChunkFile
   - ChunkFileSize
   - ChunkFileModifiedAt
   - ChunkFileHash

 Returns an error if the chunk file cannot be created, inspected, or hashed.
*/
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


/*
createChunkFile reads srcPath in ChunkSize-sized blocks, computes a SHA-256
hash for each block, and writes each hash sequentially to dstPath.

Parameters:
  - srcPath: path of the source file.
  - dstPath: path where the chunk-hash file will be created.

On failure, the partially created chunk file is removed.
*/
func (fm *FileManager) createChunkFile(srcPath string, dstPath string) error {
	buffer := make([]byte, ChunkSize)
	success := false

	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dstPath)
	if err != nil {
		return err
	}
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
