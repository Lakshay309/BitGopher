package filemanager

type FileMetadata struct {
    FileHash    []byte
    Size        int64
    ChunkSize   uint32
    TotalChunks uint32
    Filename    string
}

type FileInfo struct {
    Metadata    FileMetadata

    DisplayName string
    Keywords    []string
    Description string

    Path string
}

type FileManager struct {
	sharedDir string
	files   map[string]*FileInfo
}


func NewFileManager(sharedDir string) (*FileManager, error) {
    fm := &FileManager{
        sharedDir: sharedDir,
        files:     make(map[string]*FileInfo),
    }

    if err := fm.initialize(); err != nil {
        return nil, err
    }

    return fm, nil
}

func (fm *FileManager) initialize() error {
    // 1. Ensure shared directory exists.
	
    // 2. Scan the directory.
    // 3. Populate metadata.
    return nil
}


func (fm *FileManager) Scan() error

func (fm *FileManager) Search(query string) []*FileInfo

func (fm *FileManager) Get(hash []byte) (*FileInfo, bool)

func (fm *FileManager) ReadChunk(hash []byte, index uint32) ([]byte, error)