package filetracker

import (
	"github.com/google/uuid"
)

/*
what we want to do:
  - when current user generate a request for a file we should be able to store the state of the request
  - what should we have for one request - first thing we should have the peer who have the file ( there can be multiple of peer that have that particluar file ) we will store a list of object that contain
  - peerid (peer that has a particular file),
    that about i think we should first store in file tracker
    other thing we can store are keywords
  - this should also have states it should save thing like how the file will be downloaded
  - also have functionality like resume and stop download how much we have downlaoded a file download a particlular chunk of a file
*/
type FileTracker struct {
	fileToPeer map[string]uuid.UUID
}
