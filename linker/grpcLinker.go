package linker

import (
	"context"
	"net"
	"os"
	"strconv"

	"google.golang.org/grpc"

	"github.com/wmyi/gn/config"
	"github.com/wmyi/gn/glog"
	"github.com/wmyi/gn/gnError"
)

func NewGrpcLinker(serverId string, config *config.Config, address string, outChan chan []byte, log *glog.Glogger,
	detect *gnError.GnExceptionDetect) ILinker {
	grpc := &GrpcLinker{
		CWChan:        outChan,
		serverAddr:    address,
		serverID:      serverId,
		logger:        log,
		isRuning:      false,
		exDetect:      detect,
		clientConMaps: make(map[string]*grpc.ClientConn, 1<<4), // grpc clients  serverId:grpcClient
		conf:          config,
	}
	return grpc
}

func NewGrpcClientLinker(serverId string, config *config.Config, address string, outChan chan []byte, log *glog.Glogger,
	detect *gnError.GnExceptionDetect) ILinker {
	grpc := &GrpcLinker{
		CWChan:        outChan,
		serverAddr:    address,
		serverID:      serverId,
		logger:        log,
		isRuning:      false,
		exDetect:      detect,
		clientConMaps: make(map[string]*grpc.ClientConn, 1<<4), // grpc clients  serverId:grpcClient
		conf:          config,
	}
	return grpc
}

type GrpcLinker struct {
	CWChan        chan []byte
	serverAddr    string
	serverID      string
	logger        *glog.Glogger
	isRuning      bool
	exDetect      *gnError.GnExceptionDetect
	clientConMaps map[string]*grpc.ClientConn
	rpcServer     *grpc.Server
	conf          *config.Config
}

// push
func (gl *GrpcLinker) PushPack(pushPackServer config.GrpcNodeService_PushPackServer) error {

	return nil
}

// request  ,response rpc
func (gl *GrpcLinker) RequestRPCPack(ctx context.Context, in *config.TSession) (*config.TSession, error) {

	return nil, nil
}

func (gl *GrpcLinker) SendMsg(router string, data []byte) {
	if len(router) > 0 && len(data) > 0 && gl.conf != nil {
		clientCon, ok := gl.clientConMaps[router]
		// client
		if !ok && clientCon == nil {
			var serverAddress string = ""
			if nodeConf := gl.conf.GetConConfByServerId(router); nodeConf != nil {
				serverAddress = nodeConf.Host + ":" + strconv.Itoa(nodeConf.EndPort)
			}
			if nodeConf := gl.conf.GetServerConfByServerId(router); nodeConf != nil {
				serverAddress = nodeConf.Host + ":" + nodeConf.Port
			}
			if len(serverAddress) > 0 {
				clientCon, err := grpc.Dial(serverAddress, grpc.WithInsecure())
				if err != nil {
					gl.logger.Errorf("grpcLinker sendMsg   new Grpc client error  ", err)
				}
				gl.clientConMaps[router] = clientCon

			}
		}
		//send grpc stream pack
		if clientCon != nil {
			service := config.NewGrpcNodeServiceClient(clientCon)
		}
	}
}
func (gl *GrpcLinker) Run() error {
	if !gl.isRuning {
		var address string
		address = os.Getenv("NATS_URI")
		if len(address) <= 0 {
			address = gl.serverAddr
		}
		if len(gl.serverAddr) > 0 {
			// 启动 GRPC 服务器
			listener, err := net.Listen("tcp", gl.serverAddr)
			if err != nil {
				return err
			}
			gl.rpcServer = grpc.NewServer()
			config.RegisterGrpcNodeServiceServer(gl.rpcServer, gl)
			gl.rpcServer.Serve(listener)
		}

		gl.isRuning = true
		return nil
	} else {
		return gnError.ErrLinkerRuning
	}
}

func (gl *GrpcLinker) Done() {
	if gl.isRuning {
		gl.rpcServer.GracefulStop()
		for _, con := range gl.clientConMaps {
			con.Close()
		}
		gl.isRuning = false
	}
}
