package peer


type Peer interface {
	Start() error
	Stop() error
}