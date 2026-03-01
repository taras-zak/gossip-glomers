package main

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"sync"
	"time"

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
	Messages []int `json:"message"`
}

type State struct {
	n          *maelstrom.Node
	branching  int
	batchTimer time.Duration
	mu         sync.Mutex
	store      []int
	seen       map[int]struct{}
	peers      []string
	batcher    map[string]chan int
	wg         sync.WaitGroup
}

func NewState(n *maelstrom.Node, branching int, batchTimer time.Duration) *State {
	return &State{
		n:          n,
		branching:  branching,
		batchTimer: batchTimer,
		store:      make([]int, 0),
		seen:       make(map[int]struct{}),
		batcher:    make(map[string]chan int),
	}
}

func (s *State) handleBroadcast(msg maelstrom.Message) error {
	var body MessageBroadcast
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	_, seen := s.seen[body.Message]
	if seen {
		s.mu.Unlock()
		return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	}
	s.store = append(s.store, body.Message)
	s.seen[body.Message] = struct{}{}
	s.mu.Unlock()

	for _, peer := range s.peers {
		if peer == msg.Src || peer == s.n.ID() {
			continue
		}
		s.batcher[peer] <- body.Message
	}

	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *State) handleBroadcastBatch(msg maelstrom.Message) error {
	var body MessageBroadcastBatch
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, message := range body.Messages {
		if _, ok := s.seen[message]; ok {
			continue
		}
		s.store = append(s.store, message)
		s.seen[message] = struct{}{}

		for _, peer := range s.peers {
			if peer == msg.Src || peer == s.n.ID() {
				continue
			}
			s.batcher[peer] <- message
		}
	}

	return s.n.Reply(msg, map[string]any{"type": "broadcast_batch_ok"})
}

func (s *State) handleRead(msg maelstrom.Message) error {
	s.mu.Lock()
	messages := append([]int{}, s.store...)
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
	for _, peer := range peers {
		ch := make(chan int, 100)
		s.batcher[peer] = ch
		s.wg.Go(func() { s.runBatcher(peer, ch) })
	}
	s.mu.Unlock()

	slog.Info("received topology", slog.Any("peers", peers))
	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *State) runBatcher(peer string, ch <-chan int) {
	var batch []int
	var timerCh <-chan time.Time

	for {
		select {
		case message := <-ch:
			batch = append(batch, message)
			if len(batch) == 1 {
				timerCh = time.After(s.batchTimer)
			}
		case <-timerCh:
			msg := MessageBroadcastBatch{
				BaseMessage: BaseMessage{Type: "broadcast_batch"},
				Messages:    append([]int{}, batch...),
			}
			batch = nil
			timerCh = nil
			s.wg.Go(func() { s.sendWithRetry(peer, msg) })
		}
	}
}

func (s *State) sendWithRetry(peer string, msg any) {
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
		_, err := s.n.SyncRPC(ctx, peer, msg)
		cancel()
		if err == nil {
			slog.Info("broadcasted to peer", slog.String("peer", peer), slog.Int("attempt", attempt))
			return
		}
		slog.Error("failed to send to peer", slog.String("peer", peer), slog.String("error", err.Error()), slog.Int("attempt", attempt))
	}
}

func main() {
	n := maelstrom.NewNode()
	state := NewState(n, 5, 25*time.Millisecond)

	n.Handle("broadcast", state.handleBroadcast)
	n.Handle("broadcast_batch", state.handleBroadcastBatch)
	n.Handle("read", state.handleRead)
	n.Handle("topology", state.handleTopology)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
