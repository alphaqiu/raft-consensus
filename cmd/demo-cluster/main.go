package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"os/signal"

	"github.com/alphaqiu/demo-cluster/pkg/rpcfn"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var rootCmd = &cobra.Command{
	Use:   "demo-cluster",
	Short: "demo-cluster is a tool for managing demo-cluster",
	Long:  `demo-cluster is a tool for managing demo-cluster`,
}

// 运行节点服务，启动命令行API，启动节点rpc服务
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run is a tool for running demo-cluster",
	Run:   run,
}

// 通过调用本节点命令行API向节点添加待连接的peer，添加成功后，本节点立即发起对peer对端的连接。
// 对端peer 此时应已启动了节点。
var addPeerCmd = &cobra.Command{
	Use:   "add-peer",
	Short: "add-peer is a tool for adding a peer to the cluster",
	Run:   addPeer,
}

// 连接对端成功后，通过调用本节点命令行API向对端发起远程调用请求。
var callCmd = &cobra.Command{
	Use:   "call",
	Short: "call is a tool for calling a method on the cluster",
	Run:   call,
}

var nodeInfoCmd = &cobra.Command{
	Use:   "node-info",
	Short: "node-info is a tool for checking the node info",
	Run:   nodeInfo,
}

// 测试命令，用于测试节点是否正常工作
var nnnCmd = &cobra.Command{
	Use:   "nnn",
	Short: "nnn is a tool for testing",
	Run:   nnn,
}

var (
	logger *zap.Logger
)

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	rootCmd.PersistentFlags().String("addr", "127.0.0.1:12345", "the address to rpc listen on")
	rootCmd.PersistentFlags().String("host", "127.0.0.1:9010", "the address of the peer to add peer")
	rootCmd.AddCommand(runCmd, addPeerCmd, callCmd, nnnCmd, nodeInfoCmd)
}

func main() {
	var err error
	logger, err = zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	if err := rootCmd.Execute(); err != nil {
		logger.Error("failed to execute root command", zap.Error(err))
	}
}

func run(cmd *cobra.Command, _ []string) {
	addr, host := getAddrs(cmd)
	if host == "" {
		logger.Error("failed to get host")
		return
	}
	if addr == "" {
		logger.Error("failed to get addr")
		return
	}

	logger.Info("starting demo-cluster server")
	defer logger.Info("demo-cluster server stopped")
	server := rpcfn.NewNode(logger, addr, host)
	if err := server.Start(context.Background()); err != nil {
		logger.Error("failed to start demo-cluster server", zap.Error(err))
		return
	}
	defer server.Stop(context.Background())
	// server.AddPeer(context.Background(), "127.0.0.1:12346")
	// server.AddPeer(context.Background(), "127.0.0.1:12347")
	// server.AddPeer(context.Background(), "127.0.0.1:12348")
	// server.AddPeer(context.Background(), "127.0.0.1:12349")
	// server.AddPeer(context.Background(), "127.0.0.1:12350")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}

func addPeer(cmd *cobra.Command, args []string) {
	_, host := getAddrs(cmd)
	if host == "" {
		logger.Error("failed to get host")
		return
	}
	addr := args[0]
	resp, err := http.PostForm(fmt.Sprintf("http://%s/add-peer", host), url.Values{"addr": []string{addr}})
	if err != nil {
		logger.Error("failed to add peer", zap.Error(err))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		logger.Error("failed to add peer", zap.Error(err))
		return
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error("failed to read response body", zap.Error(err))
		return
	}
	logger.Info("add peer response", zap.String("resp", string(body)))
}

func call(cmd *cobra.Command, args []string) {
	_, host := getAddrs(cmd)
	if host == "" {
		logger.Error("failed to get host")
		return
	}
	method := args[0]
	arg := args[1:]
	resp, err := http.PostForm(fmt.Sprintf("http://%s/call-to-all", host), url.Values{"method": []string{method}, "args": arg})
	if err != nil {
		logger.Error("failed to call method", zap.Error(err))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		logger.Error("failed to call method", zap.Int("status", resp.StatusCode))
		return
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error("failed to read response body", zap.Error(err))
		return
	}
	logger.Info("call method response", zap.String("resp", string(body)))

}

func getAddrs(cmd *cobra.Command) (string, string) {
	addr, err := cmd.Flags().GetString("addr")
	if err != nil {
		logger.Error("failed to get addr", zap.Error(err))
		return "", ""
	}
	host, err := cmd.Flags().GetString("host")
	if err != nil {
		logger.Error("failed to get host", zap.Error(err))
		return "", ""
	}
	return addr, host
}

func nodeInfo(cmd *cobra.Command, _ []string) {
	_, host := getAddrs(cmd)
	if host == "" {
		logger.Error("failed to get host")
		return
	}
	resp, err := http.Get(fmt.Sprintf("http://%s/info", host))
	if err != nil {
		logger.Error("failed to get node info", zap.Error(err))
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error("failed to read response body", zap.Error(err))
		return
	}
	logger.Info("node info response", zap.String("resp", string(body)))
}

// nnn is a tool for testing
func nnn(_ *cobra.Command, _ []string) {
	logger.Info("starting demo-cluster server")
	rpcfn.New(logger)
	logger.Info("Server started, and starting mock client")

	client, err := rpc.DialHTTP("tcp", "127.0.0.1:12345")
	if err != nil {
		logger.Error("failed to dial to server", zap.Error(err))
		return
	}
	defer client.Close()

	resp := &rpcfn.HelloResponse{}
	err = client.Call("Application.Hello", &rpcfn.HelloRequest{Name: "Application is running for user: "}, resp)
	if err != nil {
		logger.Error("failed to call hello", zap.Error(err))
		return
	}
	logger.Info("hello response", zap.Int("code", resp.Reply.Code), zap.String("resp", resp.Reply.Msg))
	resp = &rpcfn.HelloResponse{}
	ch := client.Go("Application.Invoke", 10, resp, nil)
	result := <-ch.Done
	if result.Error != nil {
		logger.Error("failed to call invoke", zap.Error(result.Error))
		return
	}
	logger.Info("invoke response", zap.Int("code", resp.Reply.Code), zap.String("resp", resp.Reply.Msg))
	logger.Info("mock finished!")

}
