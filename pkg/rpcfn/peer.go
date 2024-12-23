package rpcfn

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
)

const (
	DefaultRetryTimes = 3
)

// Peer 表示一个远程网络节点, 可以对它进行连接和远程方法调用。
type Peer interface {
	ID() string
	// Connect 连接到远程节点
	Connect(ctx context.Context) error
	// Close 关闭连接
	Close(ctx context.Context) error
	// Call 调用远程方法
	Call(ctx context.Context, method string, args interface{}, reply interface{}) error
	// RemoteAddrs 返回远程节点所连接的所有节点地址
	RemoteAddrs() ([]string, error)
}

type localPeer struct {
	server  *rpc.Server
	logger  *zap.Logger
	addr    string
	runOnce sync.Once
	methods *Methods
}

func NewLocalPeer(addr string, logger *zap.Logger, resolver RemoteResolver) Peer {
	return &localPeer{logger: logger, addr: addr, methods: NewMethods(logger, resolver)}
}

func (p *localPeer) ID() string {
	return p.addr
}

func (p *localPeer) RemoteAddrs() ([]string, error) {
	return nil, nil
}

func (p *localPeer) Connect(ctx context.Context) (err error) {
	logger := p.logger
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		p.runOnce.Do(func() {
			p.server = rpc.NewServer()
			logger.Info("starting local peer: registering methods to server", zap.String("address", p.addr))
			p.server.Register(p.methods)
			listener, err := net.Listen("tcp", p.addr)
			if err != nil {
				logger.Error("failed to listen on address for starting local peer",
					zap.String("address", p.addr),
					zap.Error(err),
				)
				return
			}

			go p.server.Accept(listener)
			logger.Info("server started", zap.String("address", p.addr))
		})
	}

	return err
}

func (p *localPeer) Call(ctx context.Context, method string, req interface{}, reply interface{}) error {
	return nil
}

func (p *localPeer) Close(ctx context.Context) error { return nil }

type remotePeer struct {
	client      *rpc.Client
	logger      *zap.Logger
	addr        string
	clientMutex sync.Mutex
	connected   bool
	backoff     *backoff.ExponentialBackOff
	maxRetry    int
}

func (p *remotePeer) ID() string {
	return p.addr
}

func (p *remotePeer) RemoteAddrs() ([]string, error) {
	return []string{p.addr}, nil
}

func NewRemotePeer(addr string, logger *zap.Logger, retryTimes int) Peer {
	return &remotePeer{
		logger:   logger,
		addr:     addr,
		backoff:  backoff.NewExponentialBackOff(backoff.WithInitialInterval(100 * time.Millisecond)),
		maxRetry: retryTimes,
	}
}

func (p *remotePeer) Connect(ctx context.Context) error {
	p.clientMutex.Lock()
	defer p.clientMutex.Unlock()
	maxRetry := p.maxRetry
	if maxRetry <= 0 {
		maxRetry = DefaultRetryTimes
	}

	for i := 0; i < maxRetry; i++ {
		client, err := rpc.Dial("tcp", p.addr)
		if err == nil {
			p.client = client
			p.logger.Info("connected to remote addr", zap.String("remote", p.addr))
			p.connected = true
			return nil
		}

		time.Sleep(p.backoff.NextBackOff())
		p.logger.Error("failed to dial remote addr, retrying...",
			zap.String("remote", p.addr),
			zap.Int("retry", i+1),
			zap.Error(err),
		)
	}

	p.logger.Error("failed to connect to remote addr", zap.String("remote", p.addr))
	return errors.New("failed to connect to remote addr")
}

func (p *remotePeer) Call(ctx context.Context, method string, args interface{}, reply interface{}) error {
	p.clientMutex.Lock()
	defer p.clientMutex.Unlock()
	if !p.connected {
		return errors.New("remote peer not connected yet")
	}
	p.logger.Info("Calling remote peer -> method", zap.String("method", method), zap.Any("args", args), zap.Any("reply", reply))
	defer p.logger.Info("Called remote peer -> method", zap.String("method", method), zap.Any("args", args), zap.Any("reply", reply))
	return p.client.Call(method, args, reply)
}

func (p *remotePeer) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		defer p.logger.Info("remote connection closed")
		p.clientMutex.Lock()
		defer p.clientMutex.Unlock()
		p.connected = false
		if p.client != nil {
			_ = p.client.Close()
			p.client = nil
		}
	}
	return nil
}
