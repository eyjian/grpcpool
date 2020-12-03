// Writed by yijian on 2020/12/02
package main

import (
    "bufio"
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
    tick = flag.Uint("tick", 10000, "Tick number to print.")
    timeout = flag.Uint("timeout", 1000, "Timeout in milliseconds.")
)
var (
    gRPCPool *grpcpool.GRPCPool
    wg sync.WaitGroup
    numPendingRequests int32 // 未完成的请求数
    numFinishRequests int32 // 完成的请求数
    numCallFailedRequests int32 // 调用失败数
    numPoolFailedRequests int32 // 取池失败数
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
    numPendingRequests = int32(*numRequests)
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
        fmt.Printf("QPS: %d (Num: %d, Seconds: %d, PoolFailed: %d, CallFailed: %d)\n", qps, numFinishRequests, s, numPoolFailedRequests, numCallFailedRequests)
    } else {
        fmt.Printf("QPS: %d (Num: %d, Seconds: %d, PoolFailed: %d, CallFailed: %d) *\n", 0, numFinishRequests, s, numPoolFailedRequests, numCallFailedRequests)
    }

    fmt.Printf("\nPress ENTER to exit.\n")
    reader := bufio.NewReader(os.Stdin)
    reader.ReadString('\n')
    os.Exit(0)
}

func requestCoroutine(index int) {
    defer wg.Add(-1)

    for i:=0;;i++ {
        n := atomic.AddInt32(&numPendingRequests, -1)
        if n < 0 {
            break
        }
        finishRequests := atomic.AddInt32(&numFinishRequests, 1)
        request(index, finishRequests)
    }
}

func request(index int, finishRequests int32) {
    ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second * time.Duration(*timeout)))
    defer cancel()

    gRPCConn, errcode, err := gRPCPool.Get(ctx)
    if err != nil {
        atomic.AddInt32(&numPoolFailedRequests, 1)
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
            gRPCPool.Put(gRPCConn)
            atomic.AddInt32(&numCallFailedRequests, 1)
            if index == 0 {
                fmt.Println(err)
            }
        } else {
            gRPCPool.Put(gRPCConn)
            if needTick(finishRequests) {
                used := gRPCPool.GetUsed()
                idle := gRPCPool.GetIdle()
                fmt.Printf("(used:%d, idle:%d, finish:%d, poolfailed:%d, callfailed:%d) %s\n", used, idle, finishRequests, numPoolFailedRequests, numCallFailedRequests, res.Text)
            }
        }
    }
}

func needTick(n int32) bool {
    var need bool

    if *tick <= 0 {
        if n % 10000 == 0 {
            need = true
        } else {
            need = false
        }
    } else {
        if n % int32(*tick) == 0 {
            need = true
        } else {
            need = false
        }
    }

    return need
}