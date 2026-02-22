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

type MessageTopology struct {
	BaseMessage
	Topology map[string][]string `json:"topology"`
}

type State struct {
	Store   []int
	StoreMu sync.Mutex
	Peers   []string
	PeersMu sync.Mutex
	Seen    map[int]struct{}
	SeenMu  sync.Mutex
	wg      sync.WaitGroup
	batcher map[string]chan int
}

func NewState() *State {
	return &State{
		Store:   make([]int, 0),
		Seen:    make(map[int]struct{}),
		batcher: make(map[string]chan int),
	}
}

func main() {
	n := maelstrom.NewNode()
	state := NewState()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body MessageBroadcast
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.StoreMu.Lock()
		defer state.StoreMu.Unlock()
		if _, ok := state.Seen[body.Message]; ok {
			return n.Reply(msg, map[string]any{
				"type": "broadcast_ok",
			})
		}
		state.Store = append(state.Store, body.Message)
		state.Seen[body.Message] = struct{}{}

		for _, peer := range state.Peers {
			if peer == msg.Src || peer == n.ID() {
				continue
			}
			state.batcher[peer] <- body.Message
		}

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("broadcast_batch", func(msg maelstrom.Message) error {
		var body MessageBroadcastBatch
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		state.StoreMu.Lock()
		defer state.StoreMu.Unlock()
		for _, message := range body.Messages {
			if _, ok := state.Seen[message]; ok {
				continue
			}
			state.Store = append(state.Store, message)
			state.Seen[message] = struct{}{}

			for _, peer := range state.Peers {
				if peer == msg.Src || peer == n.ID() {
					continue
				}
				state.batcher[peer] <- message
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "broadcast_batch_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		state.StoreMu.Lock()
		defer state.StoreMu.Unlock()
		var messages []int
		for k := range state.Store {
			messages = append(messages, k)
		}

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messages,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// using our own topology
		treeTopology := tree.NewTree(n.NodeIDs(), 4)
		state.Peers = append(treeTopology.Neighbours(n.ID()), treeTopology.Parent(n.ID()))

		for _, peer := range state.Peers {
			peerChan := make(chan int, 100)
			state.batcher[peer] = peerChan
			state.wg.Go(func() {
				var batch []int
				ticker := time.NewTicker(time.Millisecond * 20)

				for {
					select {
					case message := <-peerChan:
						batch = append(batch, message)
						slog.Info("appended message to batch", slog.Int("size", len(batch)))
					case <-ticker.C:
						if len(batch) == 0 {
							slog.Info("no messages after timer")
							continue
						}
						message := MessageBroadcastBatch{
							BaseMessage: BaseMessage{
								Type: "broadcast_batch",
							},
							Messages: append([]int{}, batch...),
						}
						batch = nil
						slog.Info("sending batch")
						state.wg.Go(func() {
							attempt := 1
							for {
								ctx, cancel := context.WithTimeout(context.Background(), time.Second)
								defer cancel()

								res, err := n.SyncRPC(ctx, peer, message)
								if err == nil {
									slog.Info("broadcasted to peer successfully", slog.String("peer", peer), slog.Any("res", res), slog.Int("attempt", attempt))
									break
								}
								slog.Error("failed to send broadcast to peer", slog.String("peer", peer), slog.String("error", err.Error()), slog.Int("attempt", attempt))
								attempt++
							}
						})
					}
				}
			})
		}
		slog.Info("received topology", slog.Any("peers", state.Peers))
		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
