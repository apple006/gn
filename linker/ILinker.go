package linker

// linker
type ILinker interface {
	SendMsg(router string, data []byte)
	Run() error
	Done()
}
