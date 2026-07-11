package transport

type Transport interface{
	Start() error
	Stop() error
}