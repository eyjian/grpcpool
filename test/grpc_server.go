// Writed by yijian on 2020/12/02
package main

import (
    "context"
    "flag"
    "fmt"
    "net"
    "os"
)
import (
    "google.golang.org/grpc"
)

var (
    help = flag.Bool("h", false, "Display a help message and exit.")
    port = flag.Uint("port", 2020, "Port of gRPC server.")
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
    server := grpc.NewServer()
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
