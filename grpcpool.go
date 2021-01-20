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
	"sync"
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
	POOL_CLOSED = 4 // 连接池已被关闭
	GRPC_ERROR  = 5 // 其它 gRPC 错误
	CONN_CLOSED = 6 // 连接已关闭

	// Unavailable indicates the service is currently unavailable.
	// This is a most likely a transient condition and may be corrected
	// by retrying with a backoff. Note that it is not always safe to retry
	// non-idempotent operations.
	CONN_UNAVAILABLE = 7 // 连接被拒绝

	// DeadlineExceeded means operation expired before completion.
	// For operations that change the state of the system, this error may be
	// returned even if the operation has completed successfully. For
	// example, a successful response from a server could have been delayed
	// long enough for the deadline to expire.
	CONN_DEADLINE_EXCEEDED = 8 // 连接超时
)

// gRPC 连接
// 约束：同一 conn 不应同时被多个协程使用
type GRPCConn struct {
	endpoint string           // 服务端的端点
	closed   bool             // 为 true 表示已被关闭，这种状态的不能再使用和放回池
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
	idle     int32          // 空闲连接数（即在 clients 中的连接数）
	idleTimeout int32       // 空闲连接超时时长（单位：秒，默认值 10，可调用成员函数 SetIdleTimeout 修改）
	peakTimeout int32       // 高峰连接超时时长（单位：秒，默认值 1，可调用成员函数 SetPeakTimeout 修改，应不小于 idleTimeout 的值）
	closed      int32       // 关闭池
	accessTime  int64       // 最近一次调用 Get 或 Put 的时间，通过它可以判定是否还活跃着
	wg sync.WaitGroup // 等待 releaseIdleCoroutine 退出
	clients  chan *GRPCConn // gRPC 连接队列
	dialOpts []grpc.DialOption
}

// 方便 MetricObserver 使用
type Metric struct {
	Used int32 // 被使用连接数（不在池中数）
	Idle int32 // 空闲数连接（在池中数）

	DialRefused int32 // gRPC 拨号拒绝数
	DialTimeout int32 // gRPC 拨号超时数
	DialSuccess int32 // gRPC 拨号成功数
	DialError int32 // gRPC 拨号出错数

	GetSuccess int32 // 取池成功数
	GetEmpty int32 // 取池空数
	PutSuccess int32 // 还池成功数
	PutFull int32 // 还池满数
	PutClose int32 // 还池已关闭连接数
	PutOld int32 // 还池空闲数（长时间未使用的）
	PutIdle int32 // 还池空闲数（近期未使用的）
}

// 度量数据观察者，方便外部获取连接数等
type MetricObserver interface {
	DecUsed() int32 // 被使用连接数减一（不在池中数）
	DecIdle() int32 // 空闲数连接减一（在池中数）
	IncUsed() int32 // 被使用数增一（不在池中数）
	IncIdle() int32 // 空闲数增一（在池中数）

	IncDialRefused() int32 // gRPC 拨号拒绝数增一
	IncDialTimeout() int32 // gRPC 拨号超时数增一
	IncDialSuccess() int32 // gRPC 拨号成功数增一
	IncDialError() int32 // gRPC 拨号出错数增一（不包含拨号超时数和拒绝数）

	IncGetSuccess() int32 // 取池成功数增一（不包含新拨号的成功数）
	IncGetEmpty() int32 // 取池空数增一
	IncPutSuccess() int32 // 还池成功数增一
	IncPutFull() int32 // 还池满数增一
	IncPutClose() int32 // 还池已关闭连接数增一
	IncPutOld() int32 // 还池空闲数增一（长时间未使用的）
	IncPutIdle() int32 // 还池空闲数增一（近期未使用的）
}

// 对接口 MetricObserver 的默认实现
type DefaultMetricObserver struct {
	metric Metric
}

// Example:
// var defaultMetricObserver grpcpool.DefaultMetricObserver
// grpcpool.RegisterMetricObserver(defaultMetricObserver)
//
// 注意：
// 应在工作协程启动之前调用 RegisterMetricObserver，
// 否则会出现 metric 的 used 计数出错负值。
func RegisterMetricObserver(mo MetricObserver) {
	metricObserver = mo
}

var (
	metricObserver MetricObserver
)

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
		grpcPool.idleSize = grpcPool.initSize
	}
	grpcPool.peakSize = peakSize
	if grpcPool.peakSize < grpcPool.idleSize {
		grpcPool.peakSize = grpcPool.idleSize
	}
	grpcPool.used = 0
	grpcPool.idle = 0
	grpcPool.idleTimeout = 10
	grpcPool.peakTimeout = 2
	grpcPool.closed = 0
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
		//grpcPool.dialOpts = append(grpcPool.dialOpts, grpc.WithBlock())
		grpcPool.dialOpts = append(grpcPool.dialOpts, grpc.WithInsecure())
	}
	grpcPool.wg.Add(1)
	go grpcPool.releaseIdleCoroutine()
	return grpcPool
}

func (this *GRPCPool) GetAccessTime() int64 {
	return atomic.LoadInt64(&this.accessTime)
}

func (this *GRPCPool) SetIdleTimeout(timeout int32) {
	if timeout < 1 {
		this.idleTimeout = 1
	} else {
		this.idleTimeout = timeout
	}
}

func (this *GRPCPool) SetPeakTimeout(timeout int32) {
	if timeout < 1 {
		this.peakTimeout = 1
	} else {
		this.peakTimeout = timeout
	}
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

// 关闭连接池（释放资源）
func (this *GRPCPool) Close() {
	swapped := atomic.CompareAndSwapInt32(&this.closed, 0, 1)
	if swapped {
		closed := false

	LOOP: for {
		select {
		case conn := <-this.clients:
			if conn == nil {
				break LOOP
			}
			conn.Close()
		default:
			break LOOP
		}
	}
		if !closed {
			close(this.clients)
			closed = true
			goto LOOP
		}

		this.clients = nil
	}

	// 等待 releaseIdleCoroutine 退出
	this.wg.Wait()
}

// 从连接池取一个连接，
// 应和 Put 一对一成对调用
// 返回三个值：
// 1) GRPCConn 指针
// 2) 错误代码
// 3) 错误信息
func (this *GRPCPool) Get(ctx context.Context) (*GRPCConn, uint32, error) {
	return this.get(ctx, false)
}

func (this *GRPCPool) get(ctx context.Context, doNotNew bool) (*GRPCConn, uint32, error) {
	accessTime := time.Now().Unix()
	atomic.StoreInt64(&this.accessTime, accessTime)
	used1 := this.addUsed()

	select {
	case conn := <-this.clients:
		this.subIdle()
		if metricObserver != nil {
			metricObserver.IncGetSuccess()
		}
		return conn, SUCCESS, nil
	default:
		if doNotNew {
			return nil, SUCCESS, nil
		}
		if used1 > this.GetPeakSize() {
			used2 := this.subUsed()
			if metricObserver != nil {
				metricObserver.IncGetEmpty()
			}
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
					if metricObserver != nil {
						metricObserver.IncDialRefused()
					}
				} else if errInfo.Code() == codes.DeadlineExceeded {
					errcode = CONN_DEADLINE_EXCEEDED
					if metricObserver != nil {
						metricObserver.IncDialTimeout()
					}
				} else {
					errcode = GRPC_ERROR
					if metricObserver != nil {
						metricObserver.IncDialError()
					}
				}
				used2 := this.subUsed()
				return nil, errcode, errors.New(fmt.Sprintf("gRPC connect %s failed (used:%d, init:%d, idle:%d, peak:%d, %s)", this.endpoint, used2, this.GetInitSize(), this.GetIdleSize(), this.GetPeakSize(), err.Error()))
			} else {
				conn := new(GRPCConn)
				conn.endpoint = this.endpoint
				conn.closed = false
				conn.client = client
				conn.utime = time.Now()
				if metricObserver != nil {
					metricObserver.IncDialSuccess()
				}
				return conn, SUCCESS, nil
			}
		}
	}
}

// 连接用完后归还回池，应和 Get 一对一成对调用
// 约束：同一 conn 不应同时被多个协程使用
func (this *GRPCPool) Put(conn *GRPCConn) (uint, error) {
	return this.put(conn, false)
}

func (this *GRPCPool) put(conn *GRPCConn, doNotTouch bool) (uint, error) {
	accessTime := time.Now().Unix()
	atomic.StoreInt64(&this.accessTime, accessTime)
	defer func() {
		if err := recover(); err != nil {
			conn.Close()
			this.subIdle()
		}
	}()

	used := this.subUsed()
	closed := atomic.LoadInt32(&this.closed)
	if closed == 1 {
		if !conn.IsClosed() {
			conn.Close()
		}
		return SUCCESS, nil
	}
	if conn.IsClosed() {
		// 已关闭的不再放回池
		if metricObserver != nil {
			metricObserver.IncPutClose()
		}
		return CONN_CLOSED, nil
	} else {
		idle := this.addIdle()
		utime := conn.utime.Unix()
		if !doNotTouch {
			conn.utime = time.Now()
		}

		if idle > this.GetInitSize() {
			now := time.Now().Unix()

			if now > utime {
				itime := now - utime // idle time
				if itime > int64(this.idleTimeout) {
					conn.Close()
					this.subIdle()
					if metricObserver != nil {
						metricObserver.IncPutOld()
					}
					return POOL_IDLE, nil
				}
				if idle > this.GetIdleSize() {
					if itime > int64(this.peakTimeout) {
						conn.Close()
						this.subIdle()
						if metricObserver != nil {
							metricObserver.IncPutIdle()
						}
						return POOL_IDLE, nil
					}
				}
			}
		}
		select {
		case this.clients <- conn: // 放回连接池，如果 clients 已 closed 则会 panic。
			if metricObserver != nil {
				metricObserver.IncPutSuccess()
			}
			return SUCCESS, nil
		default:
			conn.Close()
			this.subIdle()
			if metricObserver != nil {
				metricObserver.IncPutFull()
			}
			return POOL_FULL, errors.New(fmt.Sprintf("pool for %s is full(used:%d, init:%d, idle:%d, peak:%d)", this.endpoint, used, this.GetInitSize(), this.GetIdleSize(), this.GetPeakSize()))
		}
	}
}

func (this *GRPCPool) releaseIdleCoroutine() {
	for {
		closed := atomic.LoadInt32(&this.closed)
		if closed == 1 {
			break
		}

		time.Sleep(time.Duration(1)*time.Second)
		initSize := this.GetInitSize()
		idleSize := this.GetIdle()
		usedSize := this.GetUsed()
		// 大量在使用时，表明正忙着
		if idleSize > initSize && usedSize < idleSize {
			for i:=0; i<int(idleSize); i++ {
				conn, _, _ := this.get(context.Background(), true)
				if conn == nil {
					break
				}
				errcode, _ := this.put(conn, true)
				if errcode != POOL_IDLE {
					break
				}
			}
		}
	}

	this.wg.Done()
}

func (this *GRPCPool) addUsed() int32 {
	if metricObserver != nil {
		metricObserver.IncUsed()
	}
	return atomic.AddInt32(&this.used, 1)
}

func (this *GRPCPool) subUsed() int32 {
	if metricObserver != nil {
		metricObserver.DecUsed()
	}
	return atomic.AddInt32(&this.used, -1)
}

func (this *GRPCPool) addIdle() int32 {
	if metricObserver != nil {
		metricObserver.IncIdle()
	}
	return atomic.AddInt32(&this.idle, 1)
}

func (this *GRPCPool) subIdle() int32 {
	if metricObserver != nil {
		metricObserver.DecIdle()
	}
	return atomic.AddInt32(&this.idle, -1)
}

func (this *GRPCPool) GetIdle() int32 {
	return atomic.LoadInt32(&this.idle)
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

// DefaultMetricObserver

func (this *DefaultMetricObserver) GetUsed() int32 {
	return atomic.LoadInt32(&this.metric.Used)
}

func (this *DefaultMetricObserver) GetIdle() int32 {
	return atomic.LoadInt32(&this.metric.Idle)
}

func (this *DefaultMetricObserver) DecUsed() int32 {
	return atomic.AddInt32(&this.metric.Used, -1)
}

func (this *DefaultMetricObserver) DecIdle() int32 {
	return atomic.AddInt32(&this.metric.Idle, -1)
}

func (this *DefaultMetricObserver) IncUsed() int32 {
	return atomic.AddInt32(&this.metric.Used, 1)
}

func (this *DefaultMetricObserver) IncIdle() int32 {
	return atomic.AddInt32(&this.metric.Idle, 1)
}

func (this *DefaultMetricObserver) IncDialRefused() int32 {
	return atomic.AddInt32(&this.metric.DialRefused, 1)
}

func (this *DefaultMetricObserver) IncDialTimeout() int32 {
	return atomic.AddInt32(&this.metric.DialTimeout, 1)
}

func (this *DefaultMetricObserver) IncDialSuccess() int32 {
	return atomic.AddInt32(&this.metric.DialSuccess, 1)
}

func (this *DefaultMetricObserver) IncDialError() int32 {
	return atomic.AddInt32(&this.metric.DialError, 1)
}

func (this *DefaultMetricObserver) IncGetSuccess() int32 {
	return atomic.AddInt32(&this.metric.GetSuccess, 1)
}

func (this *DefaultMetricObserver) IncGetEmpty() int32 {
	return atomic.AddInt32(&this.metric.GetEmpty, 1)
}

func (this *DefaultMetricObserver) IncPutSuccess() int32 {
	return atomic.AddInt32(&this.metric.PutSuccess, 1)
}

func (this *DefaultMetricObserver) IncPutFull() int32 {
	return atomic.AddInt32(&this.metric.PutFull, 1)
}

func (this *DefaultMetricObserver) IncPutClose() int32 {
	return atomic.AddInt32(&this.metric.PutClose, 1)
}

func (this *DefaultMetricObserver) IncPutOld() int32 {
	return atomic.AddInt32(&this.metric.PutOld, 1)
}

func (this *DefaultMetricObserver) IncPutIdle() int32 {
	return atomic.AddInt32(&this.metric.PutIdle, 1)
}

// 返回清 0 前的值
func (this *DefaultMetricObserver) ZeroDialRefused() int32 {
	return atomic.SwapInt32(&this.metric.DialRefused, 0)
}

func (this *DefaultMetricObserver) ZeroDialTimeout() int32 {
	return atomic.SwapInt32(&this.metric.DialTimeout, 0)
}

func (this *DefaultMetricObserver) ZeroDialSuccess() int32 {
	return atomic.SwapInt32(&this.metric.DialSuccess, 0)
}

func (this *DefaultMetricObserver) ZeroDialError() int32 {
	return atomic.SwapInt32(&this.metric.DialError, 0)
}

func (this *DefaultMetricObserver) ZeroGetSuccess() int32 {
	return atomic.SwapInt32(&this.metric.GetSuccess, 0)
}

func (this *DefaultMetricObserver) ZeroGetEmpty() int32 {
	return atomic.SwapInt32(&this.metric.GetEmpty, 0)
}

func (this *DefaultMetricObserver) ZeroPutSuccess() int32 {
	return atomic.SwapInt32(&this.metric.PutSuccess, 0)
}

func (this *DefaultMetricObserver) ZeroPutFull() int32 {
	return atomic.SwapInt32(&this.metric.PutFull, 0)
}

func (this *DefaultMetricObserver) ZeroPutClose() int32 {
	return atomic.SwapInt32(&this.metric.PutClose, 0)
}

func (this *DefaultMetricObserver) ZeroPutOld() int32 {
	return atomic.SwapInt32(&this.metric.PutOld, 0)
}

func (this *DefaultMetricObserver) ZeroPutIdle() int32 {
	return atomic.SwapInt32(&this.metric.PutIdle, 0)
}
