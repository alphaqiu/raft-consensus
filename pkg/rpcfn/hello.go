package rpcfn

import (
	"net"
	"net/http"
	"net/rpc"

	"go.uber.org/zap"
)

type HelloRequest struct {
	Name string `json:"name"`
	Addr string `json:"addr"` // host:port
}

type HelloResponse struct {
	*Reply
}

type Application struct {
	logger *zap.Logger
}

func (a *Application) Hello(req *HelloRequest, resp *HelloResponse) error {
	a.logger.Info("hello request -> ", zap.Any("req", req))
	resp.Reply = &Reply{
		Code: 0,
		Msg:  "Hello, " + req.Name + " from alpha",
	}
	a.logger.Info("hello response -> ", zap.Any("resp", resp))
	return nil
}

func (a *Application) Invoke(req int, resp *HelloResponse) error {
	a.logger.Info("invoke request -> ", zap.Any("req", req))
	resp.Reply = &Reply{
		Code: req,
		Msg:  "deal with invoke method.",
	}
	a.logger.Info("invoke response -> ", zap.Any("resp", resp))

	return nil
}

func New(logger *zap.Logger) {
	rpc.Register(&Application{logger: logger})
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", "127.0.0.1:12345")
	if err != nil {
		panic(err)
	}
	go http.Serve(listener, nil)
}
