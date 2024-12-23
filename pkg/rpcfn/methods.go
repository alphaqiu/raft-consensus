package rpcfn

import (
	"context"
	"slices"

	"go.uber.org/zap"
)

type RemoteResolver interface {
	RemoteAddrs() ([]string, error)
	AddPeer(ctx context.Context, addr string) error
}

func NewMethods(logger *zap.Logger, resolver RemoteResolver) *Methods {
	return &Methods{logger: logger, resolver: resolver}
}

type Methods struct {
	logger   *zap.Logger
	resolver RemoteResolver
}

func (m *Methods) Hello(req *HelloRequest, resp *HelloResponse) error {
	m.logger.Info("Starting invoke Hello method", zap.Any("req", req))
	defer m.logger.Info("End invoke Hello method", zap.Any("resp", resp))
	addrs, err := m.resolver.RemoteAddrs()
	if err != nil {
		m.logger.Error("failed to get remote addrs", zap.Error(err))
		return err
	}

	if slices.Contains(addrs, req.Addr) {
		m.logger.Info("Hello method already connected to this peer", zap.String("addr", req.Addr))
		resp.Reply = &Reply{
			Code: 0,
			Msg:  "success",
		}
		return nil
	}

	m.logger.Info("Hello method starting to add remote peer", zap.String("addr", req.Addr))
	err = m.resolver.AddPeer(context.Background(), req.Addr)
	if err != nil && err != ErrPeerAlreadyExists {
		m.logger.Error("failed to add peer", zap.Error(err))
		return nil
	}

	m.logger.Info("Hello method added remote peer successfully", zap.String("addr", req.Addr))
	resp.Reply = &Reply{
		Code: 0,
		Msg:  "success",
	}
	return nil
}

func (m *Methods) Invoke(req int, resp *HelloResponse) error {
	resp.Reply = &Reply{
		Code: req,
		Msg:  "success",
	}
	return nil
}

// RemoteAddrs 返回当前节点所连接的远程节点地址列表
// 返回的地址列表数量不超过 limit 数量。
func (m *Methods) RemoteAddrs(limit int, resp *RemoteAddrsReply) error {
	addrs, err := m.resolver.RemoteAddrs()
	if err != nil {
		return err
	}
	if limit > 0 && len(addrs) > limit {
		addrs = addrs[:limit]
	}
	resp.Addrs = addrs
	return nil
}
