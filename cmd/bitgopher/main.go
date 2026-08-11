package main

import (
	"fmt"
	"time"

	filemanager "github.com/Lakshay309/bitgopher/internal/fileManager"
	"github.com/google/uuid"
)

func main() {
	// gui := gui.NewGUI()
	// gui.Start()
	id := uuid.New()
	fm, err := filemanager.NewFileManager("E:/Coding/go-lang/BitGopher/test", id)
	if err != nil {
		fmt.Print(err)
	}
	fm.Initialize()
	// time.Sleep(20 * time.Second)
	

	fm.SeedChan <- filemanager.SeedRequest{
		Type:        filemanager.LocalSeed,
		Path:        "E:/Coding/go-lang/BitGopher/ReadMe.md",
		Description: "isWorking",
		Keywords:    []string{"test"},
		PeerID:      id,
	}
	time.Sleep(30 * time.Second)

	resp := make(chan []filemanager.FileInfo)
	fm.FileEventChan<-filemanager.FileEvent{
		Type: filemanager.GetFilesEvent,
		Metadata: filemanager.ShareMetadata{
			DisplayName: "test",
		},
		Response: resp,
	}
	result := <-resp
	fmt.Println("file manager file INfo")
	fmt.Printf("%+v\n", result)
	fm.SeedChan<-filemanager.SeedRequest{
		Type: filemanager.RemoveSeed,
		FileInfo: filemanager.FileInfo{
			DisplayName: result[0].DisplayName,
			Metadata: filemanager.FileMetadata{
				FileHash: result[0].Metadata.FileHash,
			},
		},
	}
	time.Sleep(2*time.Second)
	resp = make(chan []filemanager.FileInfo)
	fm.FileEventChan<-filemanager.FileEvent{
		Type: filemanager.GetFilesEvent,
		Metadata: filemanager.ShareMetadata{
			DisplayName: "test",
		},
		Response: resp,
	}
	result = <-resp
	fmt.Println("file manager file INfo")
	fmt.Printf("%+v\n", result)
	for {
		time.Sleep(time.Second)
	}
}
