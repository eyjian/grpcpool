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
)
import (
	"google.golang.org/grpc"
)

// gRPC 连接
type GRPCConn struct {
	endpoint string           // 服务端的端点
	closed   bool             // 为 true 表示已被关闭，这种状态的不能再使用和放回池
	inpool   bool             // 如果为 true 表示在池中
	client   *grpc.ClientConn // gRPC 连接
}

// gRPC 连接池
type GRPCPool struct {
	endpoint string         // 服务端的端点
	size     int32          // 连接池大小
	used     int32          // 已用连接数
	clients  chan *GRPCConn // gRPC 连接队列
}

// 创建 gRPC 连接池，总是返回非 nil 值，
// 注意在使用完后，应调用连接池的成员函数 Destroy 释放创建连接池时所分配的资源
func NewGRPCPool(endpoint string, size int32) *GRPCPool {
	grpcPool := new(GRPCPool)
	grpcPool.endpoint = endpoint
	grpcPool.size = size
	grpcPool.used = 0
	grpcPool.clients = make(chan *GRPCConn, size) // 在成员函数 Destroy 中释放
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

// 销毁释放连接池资源
func (this *GRPCPool) Destroy() {
	close(this.clients)
}

// 从连接池取一个连接，
// 应和 Put 一对一成对调用
func (this *GRPCPool) Get(ctx context.Context) (*GRPCConn, error) {
	used1 := atomic.AddInt32(&this.used, 1)

	select {
	case conn := <-this.clients:
		conn.inpool = false
		return conn, nil
	default:
		if used1 > this.size {
			used2 := atomic.AddInt32(&this.used, -1)
			return nil, errors.New(fmt.Sprintf("pool for %s is empty (size:%d, used:%d/%d)", this.endpoint, this.size, used1, used2))
		} else {
			client, err := grpc.DialContext(ctx, this.endpoint, grpc.WithBlock(), grpc.WithInsecure())
			if err != nil {
				return nil, errors.New(fmt.Sprintf("gRPC connect %s failed (%s)", this.endpoint, err.Error()))
			} else {
				conn := new(GRPCConn)
				conn.endpoint = this.endpoint
				conn.closed = false
				conn.inpool = false
				conn.client = client
				return conn, nil
			}
		}
	}
}

// 连接用完后归还回池，应和 Get 一对一成对调用
func (this *GRPCPool) Put(conn *GRPCConn) error {
	if conn.inpool {
		// 不能完全解决重复调用，所以应保持和 Get 的一对一成对调用关系
		return errors.New(fmt.Sprintf("gRPC connection (%s) is in pool", this.endpoint))
	} else {
		atomic.AddInt32(&this.used, -1)

		if conn.IsClosed() {
			// 已关闭的不再放回池
			return nil
		} else {
			select {
			case this.clients <- conn:
				conn.inpool = true
				return nil
			default:
				conn.inpool = false
				conn.Close()
				return errors.New(fmt.Sprintf("pool for %s is full(%d)", this.endpoint, this.size))
			}
		}
	}
}

func (this *GRPCPool) GetSize() int32 {
	return this.size
}

func (this *GRPCPool) GetUsed() int32 {
	return atomic.LoadInt32(&this.used)
}
