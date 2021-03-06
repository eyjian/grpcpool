// Writed by yijian on 2020/12/02
package main

import (
    "context"
    "flag"
    "fmt"
    "os"
    "runtime/pprof"
    "runtime/trace"
    "sync"
    "sync/atomic"
    "time"
)
import (
    "google.golang.org/grpc"
    "google.golang.org/grpc/connectivity"
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
    tick = flag.Uint("tick", 0, "Tick number to print, example: -tick=10000.")
    timeout = flag.Uint("timeout", 2000, "Timeout in milliseconds.")

    withblock = flag.Bool("withblock", false, "gRPC dial to server withblock.")
    printInterceptor = flag.Bool("print_interceptor", false, "Print interceptor information.")
)
var (
    defaultMetricObserver grpcpool.DefaultMetricObserver
    gRPCPool *grpcpool.GRPCPool
    wg sync.WaitGroup
    stopChan chan bool

    numPendingRequests int32 // 未完成的请求数
    numFinishRequests int32 // 完成的请求数
    numSuccessRequests int32 // 成功的请求数
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
    fmt.Printf("PID is %d\n", os.Getpid())

    // pprof
    profFilename := "grpc_client.prof"
    profFile, err := os.Create(profFilename)
    if err != nil {
        fmt.Printf("Create prof://%s failed: %s.\n", profFilename, err.Error())
        os.Exit(1)
    } else {
        // 生成 svg 图形文件依赖 graphviz，
        // 安装 graphviz 的命令：yum -y install graphviz
        //
        // go tool pprof grpc_client grpc_client.prof
        // 进入 pprof 后，执行 svg 命令生成 svg 格式图片文件，
        // 执行命令 top10 可查看 CPU 占用最多的前 10 个函数调用。
        //
        // 如果需要生成火焰图，则需先安装火焰图工具 go-torch，
        // 安装 go-torch 的命令：go get -v github.com/uber/go-torch
        //
        // 生成 CPU 火焰图：
        // go-torch -u http://127.0.0.1:8080  --seconds 60 -f cpu.svg
        // 生成内存火焰图：
        // go-torch  http://127.0.0.1:8080/debug/pprof/heap --colors mem -f mem.svg
        //
        // Go1.1之前需借助 go-torch 生成火焰图，之后的“go tool pprof”已集成此功能。
        pprof.StartCPUProfile(profFile)
        defer profFile.Close()
        defer pprof.StopCPUProfile()
    }
    // trace
    // go tool trace -http=:8080 grpc_client.trace
    //
    // 启动时的 DEBUG：
    // GODEBUG=schedtrace=1000 ./grpc_client # 每隔 1 秒打点一次
    // 更详细数据输出：
    // GODEBUG=schedtrace=1000,scheddetail=1 ./grpc_client
    // GODEBUG=gctrace=1 ./grpc_client
    // GODEBUG=gctrace=1,schedtrace=1000 ./grpc_client
    traceFilename := "grpc_client.trace"
    traceFile, err := os.Create(traceFilename)
    if err != nil {
        fmt.Printf("Create trace://%s failed: %s.\n", traceFilename, err.Error())
        os.Exit(1)
    } else {
        trace.Start(traceFile)
        defer traceFile.Close()
        defer trace.Stop()
    }

    stopChan = make(chan bool)
    var dialOpts []grpc.DialOption
    if *withblock {
        dialOpts = append(dialOpts, grpc.WithBlock())
    }
    dialOpts = append(dialOpts, grpc.WithInsecure())
    dialOpts = append(dialOpts, grpc.WithChainUnaryInterceptor(unaryClientInterceptor))
    gRPCPool = grpcpool.NewGRPCPool(
        *server,
        int32(*initSize),
        int32(*idleSize),
        int32(*peakSize),
        dialOpts...)
    numPendingRequests = int32(*numRequests)
    wg.Add(int(*numConcurrency))
    grpcpool.RegisterMetricObserver(&defaultMetricObserver)
    startTime := time.Now()
    for i:=0; i<int(*numConcurrency); i++ {
        go requestCoroutine(i)
    }
    if *tick == 0 {
        go metricCoroutine()
    }

    // 等待结束
    wg.Wait()
    consumeDuration := time.Since(startTime)
    time.Sleep(time.Duration(12)*time.Second)
    stopChan <-true
    close(stopChan)
    gRPCPool.Close()
    s := int(consumeDuration.Seconds())
    if s > 0 {
        qps := int(numFinishRequests) / s
        fmt.Printf("QPS: %d (Total: %d, Seconds: %d, Success: %d, PoolFailed: %d, CallFailed: %d)\n", qps, numFinishRequests, s, numSuccessRequests, numPoolFailedRequests, numCallFailedRequests)
    } else {
        fmt.Printf("QPS: %d (Total: %d, Seconds: %d, Success: %d, PoolFailed: %d, CallFailed: %d) *\n", 0, numFinishRequests, s, numSuccessRequests, numPoolFailedRequests, numCallFailedRequests)
    }
}

func metricCoroutine() {
    for ;; {
        select {
        case <-stopChan:
            return
        case <-time.After(time.Second*2): // 每隔 2 秒打点一次
            break
        }

        used := defaultMetricObserver.GetUsed()
        idle := defaultMetricObserver.GetIdle()
        dialRefused := defaultMetricObserver.ZeroDialRefused()
        dialTimeout := defaultMetricObserver.ZeroDialTimeout()
        dialSuccess := defaultMetricObserver.ZeroDialSuccess()
        dialError := defaultMetricObserver.ZeroDialError()
        getSuccess := defaultMetricObserver.ZeroGetSuccess()
        getEmpty := defaultMetricObserver.ZeroGetEmpty()

        if getSuccess > 0 || getEmpty > 0 || dialSuccess > 0 || dialRefused > 0 || dialTimeout > 0 || dialError > 0 {
            putSuccess := defaultMetricObserver.ZeroPutSuccess()
            putFull := defaultMetricObserver.ZeroPutFull()
            putClose := defaultMetricObserver.ZeroPutClose()
            putOld := defaultMetricObserver.ZeroPutOld()
            putIdle := defaultMetricObserver.ZeroPutIdle()
            fmt.Printf("Used:%d,"+
                "Idle:%d,"+
                "DialRefused:%d,"+
                "DialTimeout:%d,"+
                "DialSuccess:%d,"+
                "DialError:%d,"+
                "GetSuccess:%d,"+
                "GetEmpty:%d,"+
                "PutSuccess:%d,"+
                "PutFull:%d,"+
                "PutClose:%d,"+
                "PutOld:%d,"+
                "PutIdle:%d\n",
                used,
                idle,
                dialRefused,
                dialTimeout,
                dialSuccess,
                dialError,
                getSuccess,
                getEmpty,
                putSuccess,
                putFull,
                putClose,
                putOld,
                putIdle)
        }
    }
}

func requestCoroutine(index int) {
    defer wg.Done() // 等同于 defer wg.Add(-1)

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
    ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond * time.Duration(*timeout)))
    defer cancel()

    gRPCConn, errcode, err := gRPCPool.Get(ctx)
    if err != nil {
        atomic.AddInt32(&numPoolFailedRequests, 1)
        fmt.Printf("Get a gRPC connection from pool failed: (%d)%s\n", errcode, err.Error())
    } else {
        grpcClient := gRPCConn.GetClient()
        connState := grpcClient.GetState()
        if connState == connectivity.Connecting {
            fmt.Printf("%s is connecting\n", grpcClient.Target())
            gRPCPool.Put(gRPCConn)
            return
        } else if connState == connectivity.TransientFailure {
            fmt.Printf("%s is not connected: %s\n", grpcClient.Target(), connState.String())
            gRPCConn.Close()
            gRPCPool.Put(gRPCConn)
            return
        } else if connState != connectivity.Ready {
            fmt.Printf("%s is not connected: %s\n", grpcClient.Target(), connState.String())
            gRPCPool.Put(gRPCConn)
            return
        }
        helloClient := NewHelloServiceClient(grpcClient)
        in := HelloReq {
            Text: "Hello,Hello,Hello,Hello,Hello,Hello,Hello,Hello,Hello,Hello,Hello,Hello,Hello,Hello,Hello,Hello",
        }
        res, err := helloClient.Hello(ctx, &in)
        if err != nil {
            gRPCConn.Close()
            gRPCPool.Put(gRPCConn)
            atomic.AddInt32(&numCallFailedRequests, 1)
            if index == 0 {
                fmt.Printf("Hello to %s failed: %s\n", grpcClient.Target(), err.Error())
            }
        } else {
            gRPCPool.Put(gRPCConn)
            atomic.AddInt32(&numSuccessRequests, 1)
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

    if *tick == 0 {
        need = false
    } else {
        if n % int32(*tick) == 0 {
            need = true
        } else {
            need = false
        }
    }

    return need
}

// 拦截器
func unaryClientInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
    helloReq, ok := req.(*HelloReq)
    if *printInterceptor {
        if ok {
            fmt.Printf("FullMethod: %s with text: %s\n", method, helloReq.Text)
        } else {
            fmt.Printf("FullMethod: %s\n", method)
        }
    }
    return invoker(ctx, method, req, reply, cc, opts...)
}
