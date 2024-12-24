package rpcfn

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"sync"

	"go.uber.org/zap"
)

var (
	ErrPeerAlreadyExists = errors.New("peer already exists")
)

type Node struct {
	localPeer   Peer
	peers       map[string]Peer // address -> peer
	logger      *zap.Logger
	peerMutex   sync.Mutex
	runOnce     sync.Once
	rpcEndpoint string
	localHost   string
}

func NewNode(logger *zap.Logger, addr, host string) *Node {
	return &Node{
		logger:      logger,
		rpcEndpoint: addr,
		localHost:   host,
	}
}

func (n *Node) Start(ctx context.Context) error {
	log := n.logger
	n.runOnce.Do(func() {
		n.localPeer = NewLocalPeer(n.rpcEndpoint, log, n)
		http.HandleFunc("/add-peer", n.addPeer)
		http.HandleFunc("/call-to-all", n.callToAll)
		http.HandleFunc("/is-leader", n.isLeader)
		go http.ListenAndServe(n.localHost, nil)
	})
	log.Info("node started", zap.String("rpc-addr", n.rpcEndpoint), zap.String("local-host", n.localHost))
	return n.localPeer.Connect(ctx)
}

// RemoteAddrs returns the addresses of all currently connected remote peers
func (n *Node) RemoteAddrs() ([]string, error) {
	n.peerMutex.Lock()
	defer n.peerMutex.Unlock()
	if len(n.peers) == 0 {
		return nil, nil
	}

	allAddrs := []string{}
	for _, peer := range n.peers {
		allAddrs = append(allAddrs, peer.ID())
	}
	return allAddrs, nil
}

func (n *Node) Peers() []Peer {
	n.peerMutex.Lock()
	defer n.peerMutex.Unlock()
	peers := []Peer{}
	for _, peer := range n.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (n *Node) Stop(ctx context.Context) error {
	return n.localPeer.Close(ctx)
}

func (n *Node) AddPeer(ctx context.Context, addr string) (err error) {
	n.peerMutex.Lock()
	if n.peers == nil {
		n.peers = make(map[string]Peer)
	}
	// n.logger.Info("node rev request to starting add a peer", zap.String("addr", addr))

	if _, ok := n.peers[addr]; ok {
		defer n.peerMutex.Unlock()
		n.logger.Info("node already has this peer", zap.String("addr", addr))
		return ErrPeerAlreadyExists
	}

	peer := NewRemotePeer(addr, n.logger, DefaultRetryTimes)
	err = peer.Connect(ctx)
	if err != nil {
		defer n.peerMutex.Unlock()
		n.logger.Error("the node failed to add peer with remote addr", zap.String("remote", addr), zap.Error(err))
		return err
	}

	// n.logger.Info("node add a peer ->", zap.String("addr", addr))
	n.peers[peer.ID()] = peer
	n.peerMutex.Unlock()
	// n.logger.Info("node add a peer successfully", zap.String("addr", addr))

	// handshake and tell remote peer self ip and port
	reply := &HelloResponse{}
	addrs, err := n.RemoteAddrs()
	if err != nil {
		n.logger.Error("the node failed to get remote addrs while calling method", zap.Error(err))
		return err
	}

	if index := slices.Index(addrs, peer.ID()); index != -1 {
		addrs = append(addrs[:index], addrs[index+1:]...)
	}
	addrs = append(addrs, n.rpcEndpoint)
	err = peer.Call(ctx, "Methods.Hello", &HelloRequest{Name: "Hello", Addrs: addrs}, reply)
	if err != nil {
		n.logger.Error("the node failed to tell remote peer self ip and port, disconnect it",
			zap.String("remote", addr),
			zap.Error(err),
		)

		peer.Close(ctx)
		n.peerMutex.Lock()
		delete(n.peers, peer.ID())
		n.peerMutex.Unlock()
		return err
	}

	n.logger.Info("the node added peer with remote addr successfully", zap.String("remote", addr))
	return nil
}

func (n *Node) RemovePeer(ctx context.Context, addr string) error {
	n.peerMutex.Lock()
	defer n.peerMutex.Unlock()
	defer delete(n.peers, addr)
	return n.peers[addr].Close(ctx)
}

func (n *Node) CallToAll(ctx context.Context, method string, args interface{}, reply interface{}) error {
	n.peerMutex.Lock()
	defer n.peerMutex.Unlock()
	// n.logger.Info("node invoke n.CallToAll", zap.String("method", method), zap.Any("args", args), zap.Any("reply", reply))
	err := n.localPeer.Call(ctx, method, args, reply)
	if err != nil {
		n.logger.Error("the node failed to call method",
			zap.String("method", method),
			zap.Any("args", args),
			zap.Any("reply", reply),
			zap.Error(err),
		)
		return err
	}

	for _, peer := range n.peers {
		// n.logger.Debug("Calling node -> method", zap.String("method", method), zap.Any("args", args), zap.Any("reply", reply))
		if err := peer.Call(ctx, method, args, reply); err != nil {
			n.logger.Error("the node failed to call method", zap.String("method", method), zap.Any("args", args), zap.Any("reply", reply), zap.Error(err))
			continue
		}

		// n.logger.Info("the node called method successfully", zap.String("method", method), zap.Any("args", args), zap.Any("reply", reply))
		return nil
	}

	return nil
}

// addPeer is the simple api for the node to add a peer
func (n *Node) addPeer(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	addr := r.FormValue("addr")
	if addr == "" {
		http.Error(w, "addr is required", http.StatusBadRequest)
		return
	}

	if err := n.AddPeer(context.Background(), addr); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// callToAll is the simple api for the node to call a method to all peers
func (n *Node) callToAll(w http.ResponseWriter, r *http.Request) {
	n.logger.Info("node api calling ...")
	defer n.logger.Info("node api called ...")
	r.ParseForm()
	method := r.FormValue("method")
	args := r.FormValue("args")
	if method == "" || args == "" {
		http.Error(w, "method and args are required", http.StatusBadRequest)
		return
	}
	n.logger.Info("node prepared to call method", zap.String("method", method), zap.Any("args", args))

	switch method {
	case "Invoke":
		n.logger.Info("node prepared to call Invoke method", zap.Any("args", args))
		req, err := strconv.ParseInt(args, 10, 64)
		if err != nil {
			n.logger.Error("failed to parse args", zap.Error(err))
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		n.logger.Info("node invoke n.CallToAll", zap.Any("args", args))
		resp := &HelloResponse{}
		if err := n.CallToAll(context.Background(), "Methods."+method, req, resp); err != nil {
			n.logger.Error("call remote peer return failed", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		n.logger.Info("Invoke response", zap.Any("resp", resp))
		w.Write([]byte(strconv.Itoa(resp.Reply.Code)))
	default:
		http.Error(w, "method not found", http.StatusNotFound)
	}

}

func (n *Node) isLeader(w http.ResponseWriter, _ *http.Request) {
	if n.localPeer.IsLeader() {
		w.Write([]byte("true"))
	} else {
		w.Write([]byte("false"))
	}
}
