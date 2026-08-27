package fileManager

type FileEventType byte

const (
	AddFileEvent FileEventType = iota
	RemoveFileEvent
	GetFilesEvent
	GetFileEvent
	SearchEvent
)

const (
	MaxMetadataSize       = 10 * 1024 * 1024 // 10 Mb
	MetaDataExtensionType = ".bgmeta"
	ChunkExtensionType    = ".bgchunk"
	ChunkDir              = "chunks"
	MetaDataDir           = "metadata"
	MetadataVersion       = 1
)
