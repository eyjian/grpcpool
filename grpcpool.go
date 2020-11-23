// Writed by yijian on 2020/9/22
// gRPC 连接池实现，
// 推荐使用方式：直接源码集成。
//
// 使用方法：
// 1）调用全局函数 NewGRPCPool 创建连接池；
// 2）调用池成员函数 Get 从连接池取一个连接，如果无可用的或创建连接失败返回 nil；
// 3）使用完后调用池成员函数 Put 将连接放回连接池；
// 4）连接池不要使用后调用池成员函数 Destroy 释放连接池资源。
package grpcpool

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)
import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 错误代码
const (
	SUCCESS     = 0
	POOL_EMPTY  = 1 // 连接池空的
	POOL_FULL   = 2 // 连接池已满
	POOL_IDLE   = 3 // 连接池空闲了
	GRPC_ERROR  = 4 // 其它 gRPC 错误
	CONN_CLOSED = 5 // 连接已关闭
	CONN_INPOOL = 6 // 连接已在池中

	// Unavailable indicates the service is currently unavailable.
	// This is a most likely a transient condition and may be corrected
	// by retrying with a backoff. Note that it is not always safe to retry
	// non-idempotent operations.
	CONN_UNAVAILABLE = 6 // 连接被拒绝

	// DeadlineExceeded means operation expired before completion.
	// For operations that change the state of the system, this error may be
	// returned even if the operation has completed successfully. For
	// example, a successful response from a server could have been delayed
	// long enough for the deadline to expire.
	CONN_DEADLINE_EXCEEDED = 7 // 连接超时
)

// gRPC 连接
// 约束：同一 conn 不应同时被多个协程使用
type GRPCConn struct {
	endpoint string           // 服务端的端点
	closed   bool             // 为 true 表示已被关闭，这种状态的不能再使用和放回池
	inpool   bool             // 如果为 true 表示在池中
	client   *grpc.ClientConn // gRPC 连接
	utime    time.Time        // 最近使用时间
}

// gRPC 连接池
type GRPCPool struct {
	endpoint string         // 服务端的端点
	peakSize int32          // 连接池中高峰连接数
	idleSize int32          // 连接池较繁忙连接数
	initSize int32          // 连接池初始连接数
	used     int32          // 已用连接数
	clients  chan *GRPCConn // gRPC 连接队列
	dialOpts []grpc.DialOption
}

// 创建 gRPC 连接池，总是返回非 nil 值，
// 注意在使用完后，应调用连接池的成员函数 Destroy 释放创建连接池时所分配的资源
// 如果不指定参数 dialOpts，则默认为 grpc.WithBlock() 和 grpc.WithInsecure()。
func NewGRPCPool(endpoint string, initSize, idleSize, peakSize int32, dialOpts ...grpc.DialOption) *GRPCPool {
	grpcPool := new(GRPCPool)
	grpcPool.endpoint = endpoint
	if initSize < 1 {
		grpcPool.initSize = 1
	} else {
		grpcPool.initSize = initSize
	}
	grpcPool.idleSize = idleSize
	if grpcPool.idleSize < grpcPool.initSize {
		grpcPool.idleSize = grpcPool.initSize + 1
	}
	grpcPool.peakSize = peakSize
	if grpcPool.peakSize < grpcPool.idleSize {
		grpcPool.peakSize = grpcPool.idleSize + 1
	}
	grpcPool.used = 0
	grpcPool.clients = make(chan *GRPCConn, grpcPool.peakSize) // 在成员函数 Destroy 中释放
	grpcPool.dialOpts = make([]grpc.DialOption, len(dialOpts))
	if len(dialOpts) > 0 {
		grpcPool.dialOpts = dialOpts
	} else {
		// opts 常用可取值：
		// grpc.WithDisableHealthCheck()
		// grpc.WithDisableRetry()
		// grpc.WithDisableServiceConfig()
		// grpc.WithDefaultServiceConfig()
		// grpc.WithDefaultCallOptions()
		// grpc.WithResolvers()
		// grpc.WithAuthority()
		grpcPool.dialOpts = append(grpcPool.dialOpts, grpc.WithBlock())
		grpcPool.dialOpts = append(grpcPool.dialOpts, grpc.WithInsecure())
	}
	return grpcPool
}

func (this *GRPCConn) GetEndpoint() string {
	return this.endpoint
}

func (this *GRPCConn) GetClient() *grpc.ClientConn {
	return this.client
}

func (this *GRPCConn) Close() error {
	if this.closed {
		return nil
	} else {
		this.closed = true
		client := this.GetClient()
		return client.Close()
	}
}

func (this *GRPCConn) IsClosed() bool {
	return this.closed
}

// 销毁连接池（释放资源）
func (this *GRPCPool) Destroy() {
	close(this.clients)
	clients := this.clients
	this.clients = nil
	for client := range clients {
		client.Close()
	}
}

// 从连接池取一个连接，
// 应和 Put 一对一成对调用
// 返回三个值：
// 1) GRPCConn 指针
// 2) 错误代码
// 3) 错误信息
func (this *GRPCPool) Get(ctx context.Context) (*GRPCConn, uint32, error) {
	used1 := this.addUsed()

	select {
	case conn := <-this.clients:
		conn.inpool = false
		conn.utime = time.Now()
		return conn, SUCCESS, nil
	default:
		if used1 > this.GetPeakSize() {
			used2 := this.subUsed()
			return nil, POOL_EMPTY, errors.New(fmt.Sprintf("pool for %s is empty (used:%d/%d, init:%d, idle:%d, peak:%d)", this.endpoint, used1, used2, this.GetInitSize(), this.GetIdleSize(), this.GetPeakSize()))
		} else {
			var err error
			var client *grpc.ClientConn

			// 常见错误：
			// 1) transport: Error while dialing dial tcp 127.0.0.1:3121: connect: connection refused
			// 2) gRPC connect 127.0.0.1:3121 failed (context deadline exceeded)
			client, err = grpc.DialContext(ctx, this.endpoint, this.dialOpts[0:]...)
			if err != nil {
				var errcode uint32
				errInfo, _ := status.FromError(err)
				if errInfo.Code() == codes.Unavailable {
					errcode = CONN_UNAVAILABLE
				} else if errInfo.Code() == codes.DeadlineExceeded {
					errcode = CONN_DEADLINE_EXCEEDED
				} else {
					errcode = GRPC_ERROR
				}
				used2 := this.subUsed()
				return nil, errcode, errors.New(fmt.Sprintf("gRPC connect %s failed (used:%d, init:%d, idle:%d, peak:%d, %s)", this.endpoint, used2, this.GetInitSize(), this.GetIdleSize(), this.GetPeakSize(), err.Error()))
			} else {
				conn := new(GRPCConn)
				conn.endpoint = this.endpoint
				conn.closed = false
				conn.inpool = false
				conn.client = client
				conn.utime = time.Now()
				return conn, SUCCESS, nil
			}
		}
	}
}

// 连接用完后归还回池，应和 Get 一对一成对调用
// 约束：同一 conn 不应同时被多个协程使用
func (this *GRPCPool) Put(conn *GRPCConn) (uint, error) {
	if conn.inpool {
		// 不能完全解决重复调用，所以应保持和 Get 的一对一成对调用关系
		return CONN_INPOOL, errors.New(fmt.Sprintf("gRPC connection (%s) is in pool (used:%d, init:%d, idle:%d, peak:%d)", this.endpoint, this.GetUsed(), this.GetInitSize(), this.GetIdleSize(), this.GetPeakSize()))
	} else {
		used := this.subUsed()

		if conn.IsClosed() {
			// 已关闭的不再放回池
			return CONN_CLOSED, nil
		} else {
			if used > this.GetInitSize() {
				now := time.Now()

				utime := conn.utime.Add(time.Second*10)
				if utime.After(now) {
					// 空闲下来，释放掉超出 idle 部分的连接
					conn.Close()
					return POOL_IDLE, nil
				}
				if used > this.GetIdleSize() {
					utime := conn.utime.Add (time.Second*1)
					if utime.After(now) {
						// 空闲下来，释放掉超出 idle 部分的连接
						conn.Close()
						return POOL_IDLE, nil
					}
				}
			}

			select {
			case this.clients <- conn:
				conn.inpool = true
				return SUCCESS, nil
			default:
				conn.inpool = false
				conn.Close()
				return POOL_FULL, errors.New(fmt.Sprintf("pool for %s is full(used:%d, init:%d, idle:%d, peak:%d)", this.endpoint, used, this.GetInitSize(), this.GetIdleSize(), this.GetPeakSize()))
			}
		}
	}
}

func (this *GRPCPool) addUsed() int32 {
	return atomic.AddInt32(&this.used, 1)
}

func (this *GRPCPool) subUsed() int32 {
	return atomic.AddInt32(&this.used, -1)
}

func (this *GRPCPool) GetUsed() int32 {
	return atomic.LoadInt32(&this.used)
}

func (this *GRPCPool) GetInitSize() int32 {
	return this.initSize
}

func (this *GRPCPool) GetIdleSize() int32 {
	return this.idleSize
}

func (this *GRPCPool) GetPeakSize() int32 {
	return this.peakSize
}
