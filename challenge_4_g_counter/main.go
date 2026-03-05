package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

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

type State struct {
	n             *maelstrom.Node
	kv            *maelstrom.KV
	countersCache map[string]int
	counterKey    string
	mu            sync.Mutex
	wg            sync.WaitGroup
}

func NewState(n *maelstrom.Node) *State {
	return &State{
		n:             n,
		kv:            maelstrom.NewSeqKV(n),
		countersCache: make(map[string]int),
	}
}

func (s *State) handleAdd(msg maelstrom.Message) error {
	var body MessageAdd
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	val, err := s.kv.Read(ctx, s.counterKey)
	if err != nil {
		if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			return err
		}
		val = s.countersCache[s.counterKey]
	}

	newVal := body.Delta + val.(int)
	ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err = s.kv.Write(ctx, s.counterKey, newVal)
	if err != nil {
		return err
	}

	s.countersCache[s.counterKey] = newVal

	return s.n.Reply(msg, map[string]any{"type": "add_ok"})
}

func (s *State) handleRead(msg maelstrom.Message) error {
	resChan := make(chan int)
	wg := sync.WaitGroup{}

	s.mu.Lock()
	countersCache := make([]string, 0, len(s.countersCache))
	for k := range s.countersCache {
		countersCache = append(countersCache, k)
	}
	s.mu.Unlock()

	for _, counterKey := range countersCache {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			val, err := s.kv.Read(ctx, counterKey)
			if err != nil {
				slog.Error("failed to get counter value", slog.String("counter", counterKey), slog.Any("error", err))
				s.mu.Lock()
				val = s.countersCache[counterKey]
				s.mu.Unlock()
			}
			s.mu.Lock()
			s.countersCache[counterKey] = max(val.(int), s.countersCache[counterKey])
			s.mu.Unlock()
			resChan <- val.(int)
		})
	}
	s.wg.Go(func() {
		wg.Wait()
		close(resChan)
	})
	sum := 0
	for val := range resChan {
		sum += val
	}
	return s.n.Reply(msg, map[string]any{
		"type":  "read_ok",
		"value": sum,
	})
}

func (s *State) handleInit(_ maelstrom.Message) error {
	s.counterKey = fmt.Sprintf("counter_%s", s.n.ID())
	for _, n := range s.n.NodeIDs() {
		//TODO: if node recovers from crash, it must restore state from KV
		s.countersCache[fmt.Sprintf("counter_%s", n)] = 0
	}
	return nil
}

func main() {
	n := maelstrom.NewNode()
	state := NewState(n)

	n.Handle("add", state.handleAdd)
	n.Handle("read", state.handleRead)
	n.Handle("init", state.handleInit)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
