package main

import (
	"encoding/json"
	"log"
	"log/slog"
	"sync"

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
}

func NewState() *State {
	return &State{
		Store: make([]int, 0),
		Seen:  make(map[int]struct{}),
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
			err := n.Send(peer, body)
			if err != nil {
				slog.Error("failed to send broadcast to peer", slog.String("peer", peer))
			}
		}

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		state.StoreMu.Lock()
		defer state.StoreMu.Unlock()
		var messages []int
		for _, k := range state.Store {
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

		state.PeersMu.Lock()
		defer state.PeersMu.Unlock()
		state.Peers = body.Topology[n.ID()]
		slog.Info("received topology", slog.Any("peers", body.Topology[n.ID()]))
		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
