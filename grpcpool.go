// Writed by yijian on 2020/9/22
// gRPC 连接池实现，方便协程中使用长连接
package grpcpool

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
)
import (
	"google.golang.org/grpc"
)

// gRPC 连接
type GRPCConn struct {
	endpoint string
	closed   bool // 为 true 表示已被关闭，这种状态的不能再使用和放回池
	client   *grpc.ClientConn
}

// gRPC 连接池
type GRPCPool struct {
	endpoint string // 服务端的端点
	size     int32  // 连接池大小
	used     int32  // 已用连接数

	mtx     sync.Mutex
	clients list.List
}

// 创建 gRPC 连接池
func NewGRPCPool(endpoint string, size int32) *GRPCPool {
	grpcPool := new(GRPCPool)
	grpcPool.endpoint = endpoint
	grpcPool.size = size
	grpcPool.used = 0
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

// 从连接池取一个连接
func (this *GRPCPool) Get(ctx context.Context) (*GRPCConn, error) {
	this.mtx.Lock()
	defer this.mtx.Unlock()

	if this.clients.Len() > 0 {
		element := this.clients.Back()
		conn := (element.Value).(*GRPCConn)
		this.clients.Remove(element)
		atomic.AddInt32(&this.used, 1)
		return conn, nil
	} else {
		client, err := grpc.DialContext(ctx, this.endpoint, grpc.WithBlock(), grpc.WithInsecure())
		if err != nil {
			return nil, err
		} else {
			conn := new(GRPCConn)
			conn.endpoint = this.endpoint
			conn.closed = false
			conn.client = client
			return conn, nil
		}
	}
}

// 连接用完后归还回池
func (this *GRPCPool) Put(conn *GRPCConn) {
	if !conn.IsClosed() {
		this.mtx.Lock()
		defer this.mtx.Unlock()
		this.clients.PushFront(conn)
		atomic.AddInt32(&this.used, -1)
	}
}
