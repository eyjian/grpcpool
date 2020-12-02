// Writed by yijian on 2020/12/02
package main

import (
    "context"
    "flag"
    "fmt"
    "os"
    "sync"
    "sync/atomic"
    "time"
)
import (
    "github.com/eyjian/grpcpool"
)

var (
    help = flag.Bool("h", false, "Display a help message and exit.")
    server = flag.String("server", "127.0.0.1:2020", "Server of gRPC to connect.")
    initSize = flag.Int("init_size", 1, "Initial size of gRPC pool.")
    idleSize = flag.Int("idle_size", 10, "Idle size of gRPC pool.")
    peakSize = flag.Int("peak_size", 100, "Peak size of gRPC pool.")

    numRequests = flag.Uint("n", 1, "Number of requests to perform.")
    numConcurrency = flag.Uint("c", 1, "Number of multiple requests to make at a time.")
)
var (
    gRPCPool *grpcpool.GRPCPool
    wg sync.WaitGroup
    numPendingRequests int32 // 未完成的请求数
    numFinishRequests int32 // 完成的请求数
    numFailedRequests int32 // 请求失败数
)

func main() {
    flag.Parse()
    if *help {
        flag.Usage()
        os.Exit(1)
    }

    if *server == "" {
        fmt.Printf("Parameter[-server] is not set.\n")
        flag.Usage()
        os.Exit(1)
    }

    gRPCPool = grpcpool.NewGRPCPool(*server, int32(*initSize), int32(*idleSize), int32(*peakSize))
    atomic.AddInt32(&numPendingRequests, int32(*numRequests))
    wg.Add(int(*numConcurrency))
    startTime := time.Now()
    for i:=0; i<int(*numConcurrency); i++ {
        go requestCoroutine(i)
    }
    wg.Wait()
    consumeDuration := time.Since(startTime)
    s := int(consumeDuration.Seconds())
    if s > 0 {
        qps := int(numFinishRequests) / s
        fmt.Printf("QPS: %d (Num: %d, Seconds: %d, Failed: %d)\n", qps, numFinishRequests, s, numFailedRequests)
    } else {
        fmt.Printf("QPS: %d (Num: %d, Seconds: %d, Failed: %d) *\n", 0, numFinishRequests, s, numFailedRequests)
    }
}

func requestCoroutine(index int) {
    defer wg.Add(-1)

    for i:=0;;i++ {
        n := atomic.AddInt32(&numPendingRequests, -1)
        if n < 0 {
            break
        }
        atomic.AddInt32(&numFinishRequests, 1)
        ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Second))
        defer cancel()

        gRPCConn, errcode, err := gRPCPool.Get(ctx)
        if err != nil {
            atomic.AddInt32(&numFailedRequests, 1)
            fmt.Printf("Get a gRPC connection from pool failed: (%d)%s\n", errcode, err.Error())
        } else {
            grpcClient := gRPCConn.GetClient()
            helloClient := NewHelloServiceClient(grpcClient)
            in := HelloReq {
                Text: "Hello",
            }
            res, err := helloClient.Hello(ctx, &in)
            if err != nil {
                gRPCConn.Close()
                atomic.AddInt32(&numFailedRequests, 1)
                if index == 0 {
                    fmt.Println(err)
                }
            } else {
                gRPCPool.Put(gRPCConn)
                if index == 0 && i%10000 == 0 {
                    fmt.Printf("%s\n", res.Text)
                }
            }
        }
    }
}
