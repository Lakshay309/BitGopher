package fileManager

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

type FileEvent struct {
	Type     FileEventType
	Metadata ShareMetadata
	index    int64
	FileHash []byte
	Response chan FileEventResponse
}

type FileEventResponse struct {
	FileInfos []FileInfo
	Err       error
	DataBytes []byte
}
