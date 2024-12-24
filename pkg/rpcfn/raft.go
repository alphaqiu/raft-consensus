package rpcfn

import (
	"context"
	"time"

	"math/rand"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	DefaultHeartbeatInterval = 100 * time.Millisecond
)

type NodeState int

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	}

	return "Unknown"
}

const (
	Follower NodeState = iota
	Leader
	Candidate
)

func NewConsensus(id string, logger *zap.Logger, resolver RemoteResolver) *RaftConsensus {
	return &RaftConsensus{
		id:            id,
		logger:        logger,
		resolver:      resolver,
		resetElection: make(chan struct{}, 1),
		quit:          make(chan struct{}),
		term:          atomic.NewUint64(0),
		index:         atomic.NewUint64(0),
		votedFor:      atomic.NewString(""),
		lastUpdated:   atomic.NewTime(time.Now()),
		votes:         atomic.NewInt64(0),
	}
}

type RaftConsensus struct {
	id          string
	state       atomic.Value   // 节点状态
	term        *atomic.Uint64 // 当前任期
	index       *atomic.Uint64 // 当前节点日志复制的索引
	votedFor    *atomic.String // 在当前term中投给谁的票
	lastUpdated *atomic.Time   // 上次更新时间
	votes       *atomic.Int64  // 当前节点收到的投票数

	resolver RemoteResolver

	quit          chan struct{}
	resetElection chan struct{}
	electionTimer *time.Timer

	logger *zap.Logger
}

func (r *RaftConsensus) Start() {
	defer r.logger.Info("raft consensus started", zap.String("id", r.id))
	r.state.Store(Follower)
	go r.observe()
	go r.heartbeat()
}

func (r *RaftConsensus) Stop() {
	close(r.quit)
}

// randomTimeout returns a random duration between 150ms and 300ms
// 这个实现:
// 使用 rand.Intn(151) 生成 [0,150] 范围内的随机整数
// 加上 150 使范围变为 [150,300]
// 将结果转换为毫秒级的 Duration
func randomTimeout() time.Duration {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Duration(rng.Intn(151)+150) * time.Millisecond
}

func (r *RaftConsensus) observe() {
	rt := randomTimeout()
	r.logger.Info("raft consensus observe starting", zap.Duration("election-timeout", rt))
	r.electionTimer = time.NewTimer(rt)
	defer r.electionTimer.Stop()

	for {
		select {
		case <-r.quit:
			r.logger.Info("raft consensus observe stopped")
			return
		case <-r.resetElection:
			rt = randomTimeout()
			// r.logger.Debug("raft consensus observe reset election timer", zap.Duration("election-timeout", rt))
			r.electionTimer.Reset(rt)
		case <-r.electionTimer.C:
			// r.logger.Debug("raft consensus observe election timer timeout, checking if need to elect",
			// 	zap.String("id", r.id),
			// 	zap.Any("state", r.state.Load()),
			// 	zap.Duration("last-updated", time.Since(r.lastUpdated.Load())),
			// )
			if r.state.CompareAndSwap(Follower, Candidate) &&
				time.Since(r.lastUpdated.Load()) > DefaultHeartbeatInterval {
				r.elect()
			}
			rt = randomTimeout()
			// r.logger.Info("raft consensus observe reset election timer", zap.Duration("election-timeout", rt))
			r.electionTimer.Reset(rt)
		}
	}
}

// 发起选举 and send vote request to all peers (RequestVote RPC)
// If the receiving node hasn't voted yet in this term then it votes for the candidate.
// and the node resets its election timeout.
// Once a candidate has a majority of votes it becomes leader.
// The leader begins sending out Append Entries messages to its followers. (AppendEntries RPC)
// These messages are sent in intervals specified by the *heartbeat timeout*.
// Followers then respond to each Append Entries message.
// This election term will continue until a follower stops receiving heartbeats and becomes a candidate.
func (r *RaftConsensus) elect() {
	// defer r.logger.Info("raft consensus elect finished", zap.String("id", r.id))
	peers := r.resolver.Peers()
	if len(peers) <= 1 { // 最少3个节点，包括自身；所连接的节点不能少于1个，集群节点总和最少3个节点。
		// r.logger.Info("raft consensus elect has not enough peers, go back to follower",
		// 	zap.String("id", r.id),
		// 	zap.Int("peers", len(peers)),
		// )
		r.state.Store(Follower)
		return
	}

	r.term.Inc()
	r.votes.Inc()
	ctx, cancel := context.WithTimeout(context.Background(), DefaultHeartbeatInterval)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)

	results := make(chan *RequestVoteResponse, len(peers))
	r.logger.Info("raft consensus elect starting",
		zap.String("id", r.id),
		zap.Int("peers", len(peers)),
		zap.Uint64("term", r.term.Load()),
	)
	totalPeers := atomic.NewInt64(int64(len(peers)))
	for _, peer := range peers {
		peer := peer
		group.Go(func() error {
			resp := &RequestVoteResponse{}
			err := peer.Call(ctx, "RaftConsensus.RequestVote", &RequestVoteRequest{
				Term:        r.term.Load(),
				CandidateId: r.id,
			}, resp)
			if err != nil {
				r.logger.Info("raft consensus elect received vote response, peer call error", zap.String("id", r.id), zap.Error(err))
				results <- resp
				r.resolver.RemovePeer(ctx, peer.ID())
				totalPeers.Dec()
				return err
			}

			select {
			case <-ctx.Done():
				r.logger.Info("raft consensus elect received vote response, context done", zap.String("id", r.id))
				results <- resp
				r.resolver.RemovePeer(ctx, peer.ID())
				totalPeers.Dec()
				return ctx.Err()
			case results <- resp:
				// r.logger.Info("raft consensus elect received vote response",
				// 	zap.Any("result", resp),
				// 	zap.String("local-id", r.id),
				// 	zap.String("remote-id", peer.ID()),
				// )
				return nil
			}
		})
	}

	for result := range results {
		// r.logger.Info("raft consensus elect received vote response", zap.Any("result", result))
		if !result.VoteGranted && result.Term > r.term.Load() {
			r.logger.Info("raft consensus elect received vote response, term is greater than current term, go back to *Follower*",
				zap.String("id", r.id),
				zap.Uint64("result-term", result.Term),
				zap.Uint64("current-term", r.term.Load()),
			)
			r.term.Store(result.Term)
			r.votedFor.Store("")
			r.votes.Store(0)
			r.lastUpdated.Store(time.Now())
			r.state.Store(Follower)
			return
		} else if !result.VoteGranted {
			r.logger.Info("raft consensus elect received vote response, vote not granted +1, continue", zap.String("id", r.id))
			continue
		}

		r.votes.Inc()
		majority := int64((totalPeers.Load() + 1) / 2)
		if r.votes.Load() > majority {
			r.logger.Info("won the election, become *LEADER*",
				zap.String("id", r.id),
				zap.Int64("votes", r.votes.Load()),
				zap.Int64("majority", majority),
				zap.Uint64("term", r.term.Load()),
			)
			r.state.Store(Leader)
			r.votes.Store(0)
			r.lastUpdated.Store(time.Now())
			r.votedFor.Store("")

			ctx, cancel := context.WithTimeout(context.Background(), DefaultHeartbeatInterval)
			r.sendHeartbeat(ctx)
			cancel()

			return
		}
	}

	// r.logger.Info("raft consensus elect received vote response, not enough votes, go back to follower", zap.String("id", r.id))
	r.state.Store(Follower)
	r.votes.Store(0)
	r.lastUpdated.Store(time.Now())
	r.votedFor.Store("")

}

func (r *RaftConsensus) heartbeat() {
	r.logger.Info("raft consensus heartbeat starting", zap.String("id", r.id))
	// Leader 节点应当以多少的速率发送心跳包？
	// 1. 如果 Leader 节点发送心跳包的速率太快，可能会导致 Follower 节点频繁地更新自己的状态，从而导致网络拥塞。
	// 2. 如果 Leader 节点发送心跳包的速率太慢，可能会导致 Follower 节点无法及时地发现 Leader 节点的故障，从而导致整个集群的稳定性受到影响。
	heartbeatTimer := time.NewTimer(DefaultHeartbeatInterval)
	defer heartbeatTimer.Stop()

	for {
		select {
		case <-r.quit:
			r.logger.Info("raft consensus heartbeat stopped", zap.String("id", r.id))
			return
		case <-heartbeatTimer.C:
			if r.state.Load() != Leader {
				// r.logger.Info("raft consensus heartbeat not a leader, skip heartbeat to followers", zap.String("id", r.id))
				heartbeatTimer.Reset(DefaultHeartbeatInterval)
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), DefaultHeartbeatInterval)
			r.sendHeartbeat(ctx)
			cancel()
			heartbeatTimer.Reset(DefaultHeartbeatInterval)
		}
	}
}

func (r *RaftConsensus) sendHeartbeat(ctx context.Context) {
	// r.logger.Info("raft consensus send heartbeat to followers", zap.String("id", r.id))
	peers := r.resolver.Peers()
	group, ctx := errgroup.WithContext(ctx)
	for _, peer := range peers {
		peer := peer
		group.Go(func() error {
			resp := &AppendEntriesResponse{}
			err := peer.Call(ctx, "RaftConsensus.AppendEntries", &AppendEntriesRequest{
				Term:         r.term.Load(),
				LeaderId:     r.id,
				LeaderCommit: r.index.Load(),
			}, resp)
			if err != nil {
				r.logger.Error("raft consensus send heartbeat to follower failed", zap.String("remote-id", peer.ID()), zap.Error(err))
				r.resolver.RemovePeer(ctx, peer.ID())
				return err
			}
			return nil
		})
	}
	group.Wait()
}

func (r *RaftConsensus) RequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) error {
	r.logger.Info("raft consensus received from remote candinate request for voting it",
		zap.String("local-id", r.id),
		zap.String("remote-id", req.CandidateId),
		zap.Uint64("remote-term", req.Term),
		zap.Uint64("local-term", r.term.Load()),
		zap.String("local-voted-for", r.votedFor.Load()),
	)
	if r.state.Load() == Follower &&
		r.term.Load() < req.Term &&
		r.votedFor.Load() == "" {
		r.logger.Info("raft consensus received from remote candinate request for voting it, vote *granted*",
			zap.String("id", r.id),
			zap.String("remote-id", req.CandidateId),
		)
		r.votedFor.Store(req.CandidateId)
		resp.VoteGranted = true
		resp.Term = r.term.Load()
		r.resetElection <- struct{}{}
		return nil
	}
	resp.VoteGranted = false
	resp.Term = r.term.Load()
	r.logger.Info("raft consensus received from remote candinate request for voting it, vote *not granted*",
		zap.String("id", r.id),
		zap.String("remote-id", req.CandidateId),
	)
	return nil
}

func (r *RaftConsensus) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	switch r.state.Load() {
	case Candidate:
		r.logger.Info("raft consensus received from remote leader request for append entries, become *FOLLOWER*",
			zap.String("id", r.id),
		)
		r.state.Store(Follower)
		fallthrough
	case Follower:
		// r.logger.Info("raft consensus received from remote leader request for append entries, self is a follower",
		// 	zap.String("id", r.id),
		// )
		r.lastUpdated.Store(time.Now())
		r.term.Store(req.Term)
		r.votedFor.Store("")
		r.votes.Store(0)
		resp.Success = true
		resp.Term = r.term.Load()
		r.resetElection <- struct{}{}
	case Leader:
		r.logger.Info("raft consensus received from remote leader request for append entries, self is a leader",
			zap.String("id", r.id),
		)
		resp.Term = r.term.Load()
		resp.Success = false
	}
	return nil
}

type RequestVoteRequest struct {
	Term        uint64
	CandidateId string
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderId     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

type LogEntry struct {
	Term    uint64
	Command interface{}
}
