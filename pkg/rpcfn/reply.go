package rpcfn

type Reply struct {
	Code int
	Msg  string
}

type RemoteAddrsReply struct {
	Reply
	Addrs []string
}

func NewRemoteAddrsReply(addrs []string) *RemoteAddrsReply {
	return &RemoteAddrsReply{Reply: Reply{Code: 0, Msg: "success"}, Addrs: addrs}
}
