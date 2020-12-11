// Writed by yijian on 2020/12/02
package main

import (
    "context"
    "flag"
    "fmt"
    "google.golang.org/grpc/tap"
    "net"
    "os"
    "time"
)
import (
    "google.golang.org/grpc"
)

var (
    help = flag.Bool("h", false, "Display a help message and exit.")
    port = flag.Uint("port", 2020, "Port of gRPC server.")
    printInterceptor = flag.Bool("print_interceptor", false, "Print interceptor information.")
)

func main() {
    flag.Parse()
    if *help {
        flag.Usage()
        os.Exit(1)
    }

    listen, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
    if err != nil {
        fmt.Printf("gRPC listen on %d failed: %s\n", *port, err.Error())
        os.Exit(1)
    }

    fmt.Printf("gRPC listen on %d, PID is %d\n", *port, os.Getpid())
    server := grpc.NewServer(
        grpc.ChainUnaryInterceptor(
            unaryInterceptor),
        grpc.ServerOption(
            grpc.ConnectionTimeout(time.Millisecond*time.Duration(1000))),
        grpc.InTapHandle(serverInHandle),)
    RegisterHelloServiceServer(server, &gRPCHandler{})
    err = server.Serve(listen)
    if err != nil {
        fmt.Printf("Started gRPC server failed: %s\n", err.Error())
    }
}

// 对接口 HelloServiceServer 的实现
type gRPCHandler struct {
}

func (this *gRPCHandler) Hello(context.Context, *HelloReq) (*HelloRes, error) {
    res := HelloRes {
        Text: "World",
    }
    return &res, nil
}

// returns a non-nil error, the stream will not be
// created and a RST_STREAM will be sent back to the client with REFUSED_STREAM.
// The client will receive an RPC error "code = Unavailable, desc = stream
// terminated by RST_STREAM with error code: REFUSED_STREAM".
func serverInHandle(ctx context.Context, info *tap.Info) (context.Context, error) {
    // 每次 gRPC 请求均会调用，在 IO 协程中完成，因此不能有任何慢操作
    // FullMethodName is the string of grpc method (in the format of /package.service/method).
    //fmt.Printf("FullMethodName: %s\n", info.FullMethodName)
    return ctx, nil
}

// 拦截器
func unaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
    if *printInterceptor {
        helloReq, ok := req.(*HelloReq)
        if ok {
           fmt.Printf("FullMethod: %s with text: %s\n", info.FullMethod, helloReq.Text)
        } else {
           fmt.Printf("FullMethod: %s\n", info.FullMethod)
        }
    }
    return handler(ctx, req)
}
