package initial

import (
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/easy-oj/common/logs"
	"github.com/easy-oj/common/proto/queue"
	"github.com/easy-oj/common/settings"
	"github.com/easy-oj/queue/service"
)

func Initialize() {
	address := fmt.Sprintf("0.0.0.0:%d", settings.Queue.Port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer()
	queue.RegisterQueueServiceServer(server, service.NewQueueHandler())
	reflection.Register(server)
	go func() {
		if err := server.Serve(lis); err != nil {
			panic(err)
		}
	}()
	logs.Info("[Initialize] service served on %s", address)
}
