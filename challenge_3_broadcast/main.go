package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type State struct {
	Store   map[int]struct{}
	StoreMu sync.RWMutex
	Peers   []string
}

func NewState() *State {
	return &State{
		Store: make(map[int]struct{}),
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

		state.StoreMu.RLock()
		_, ok := state.Store[body.Message]
		state.StoreMu.RUnlock()
		if ok {
			return nil
		}

		for _, peer := range state.Peers {
			go func(dst string) {
				for {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					_, err := n.SyncRPC(ctx, dst, map[string]any{
						"type":    "broadcast",
						"message": body.Message,
					})
					if err == nil {
						break
					}
					log.Printf("failed broadcast message to peer %s, err: %e", dst, err)
				}
			}(peer)
		}

		state.StoreMu.Lock()
		defer state.StoreMu.Unlock()
		state.Store[body.Message] = struct{}{}

		if body.MsgID != 0 {
			return n.Reply(msg, map[string]any{
				"type": "broadcast_ok",
			})
		}
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		state.StoreMu.RLock()
		defer state.StoreMu.RUnlock()
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
		var body MessageTopology
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		state.Peers = body.Topology[n.ID()]
		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}

type BaseMessage struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
}

type MessageTopology struct {
	BaseMessage
	Topology map[string][]string `json:"topology"`
}

type MessageBroadcast struct {
	BaseMessage
	Message int `json:"message"`
}
