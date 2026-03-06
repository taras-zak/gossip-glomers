package main

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"maps"
	"sync"
	"time"

	"gossip-glomers/internal/crdt"
	"gossip-glomers/internal/tree"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BaseMessage struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
}

type MessageBroadcast struct {
	BaseMessage
	Message int `json:"message"`
}

type MessageBroadcastBatch struct {
	BaseMessage
	Set crdt.GSet `json:"set"`
}

type State struct {
	n                *maelstrom.Node
	branching        int
	gossipTicker     time.Duration
	maxRetryAttempts int
	requestTimeout   time.Duration
	retryWait        time.Duration
	mu               sync.Mutex
	store            crdt.GSet
	peers            []string
	wg               sync.WaitGroup
}

func NewState(n *maelstrom.Node, branching int, batchTimer time.Duration) *State {
	return &State{
		n:                n,
		branching:        branching,
		gossipTicker:     batchTimer,
		maxRetryAttempts: 3,
		requestTimeout:   300 * time.Millisecond,
		retryWait:        100 * time.Millisecond,
		store:            make(crdt.GSet),
	}
}

func (s *State) handleBroadcast(msg maelstrom.Message) error {
	var body MessageBroadcast
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	s.store.Add(body.Message)
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *State) handleBroadcastBatch(msg maelstrom.Message) error {
	var body MessageBroadcastBatch
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.store.Merge(body.Set)

	return s.n.Reply(msg, map[string]any{"type": "broadcast_batch_ok"})
}

func (s *State) handleRead(msg maelstrom.Message) error {
	s.mu.Lock()
	messages := s.store.Elements()
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
	})
}

func (s *State) handleTopology(msg maelstrom.Message) error {
	treeTopology := tree.NewTree(s.n.NodeIDs(), s.branching)
	peers := append(treeTopology.Children(s.n.ID()), treeTopology.Parent(s.n.ID()))

	s.mu.Lock()
	s.peers = peers
	s.mu.Unlock()

	s.wg.Go(func() { s.runGossip() })

	slog.Info("received topology", slog.Any("peers", peers))
	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *State) runGossip() {
	ticker := time.NewTicker(s.gossipTicker)

	for range ticker.C {
		s.mu.Lock()
		countersCopy := make(crdt.GSet)
		maps.Copy(countersCopy, s.store)
		s.mu.Unlock()

		msg := MessageBroadcastBatch{
			BaseMessage: BaseMessage{Type: "broadcast_batch"},
			Set:         countersCopy,
		}
		for _, peer := range s.peers {
			s.wg.Go(func() { s.sendWithRetry(peer, msg) })
		}
	}
}

func (s *State) sendWithRetry(peer string, msg any) {
	for attempt := 1; attempt < s.maxRetryAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), s.requestTimeout)
		_, err := s.n.SyncRPC(ctx, peer, msg)
		cancel()
		if err == nil {
			slog.Info("broadcasted to peer", slog.String("peer", peer), slog.Int("attempt", attempt))
			return
		}
		slog.Error("failed to send to peer", slog.String("peer", peer), slog.String("error", err.Error()), slog.Int("attempt", attempt))
		time.Sleep(s.retryWait)
	}
}

func main() {
	n := maelstrom.NewNode()
	state := NewState(n, 25, 200*time.Millisecond)

	n.Handle("broadcast", state.handleBroadcast)
	n.Handle("broadcast_batch", state.handleBroadcastBatch)
	n.Handle("read", state.handleRead)
	n.Handle("topology", state.handleTopology)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
