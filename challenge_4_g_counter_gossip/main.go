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

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BaseMessage struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
}

type MessageAdd struct {
	BaseMessage
	Delta int `json:"delta"`
}

type MessageBroadcastCounters struct {
	BaseMessage
	Counters crdt.GCounter `json:"counters"`
}

type State struct {
	n       *maelstrom.Node
	counter crdt.GCounter
	peers   []string
	mu      sync.Mutex
	wg      sync.WaitGroup

	requestTimeout        time.Duration
	broadcastCountersTick time.Duration
	maxRetryAttempts      int
	retryWait             time.Duration
}

func NewState(n *maelstrom.Node) *State {
	return &State{
		n:                     n,
		counter:               make(crdt.GCounter),
		requestTimeout:        600 * time.Millisecond,
		broadcastCountersTick: time.Second,
		maxRetryAttempts:      5,
		retryWait:             time.Millisecond * 100,
	}
}

func (s *State) handleAdd(msg maelstrom.Message) error {
	var body MessageAdd
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.counter.Increment(s.n.ID(), body.Delta)

	return s.n.Reply(msg, map[string]any{"type": "add_ok"})
}

func (s *State) handleGossip(msg maelstrom.Message) error {
	var body MessageBroadcastCounters
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.counter.Merge(body.Counters)

	return s.n.Reply(msg, map[string]any{"type": "broadcast_counters_ok"})
}

func (s *State) handleRead(msg maelstrom.Message) error {
	s.mu.Lock()
	sum := s.counter.Value()
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type":  "read_ok",
		"value": sum,
	})
}

func (s *State) handleInit(_ maelstrom.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, n := range s.n.NodeIDs() {
		if n == s.n.ID() {
			continue
		}
		s.counter.Increment(n, 0)
		s.peers = append(s.peers, n)
	}
	s.wg.Go(s.runGossip)
	return nil
}

func (s *State) runGossip() {
	ticker := time.NewTicker(s.broadcastCountersTick)

	for range ticker.C {
		s.mu.Lock()
		countersCopy := make(crdt.GCounter)
		maps.Copy(countersCopy, s.counter)
		s.mu.Unlock()

		msg := MessageBroadcastCounters{
			BaseMessage: BaseMessage{Type: "broadcast_counters"},
			Counters:    countersCopy,
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
	state := NewState(n)

	n.Handle("add", state.handleAdd)
	n.Handle("read", state.handleRead)
	n.Handle("broadcast_counters", state.handleGossip)
	n.Handle("init", state.handleInit)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
