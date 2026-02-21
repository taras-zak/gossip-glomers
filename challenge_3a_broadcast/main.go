package main

import (
	"encoding/json"
	"log"
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

type State struct {
	Store   []int
	StoreMu sync.Mutex
}

func NewState() *State {
	return &State{
		Store: make([]int, 0),
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
		state.Store = append(state.Store, body.Message)
		state.StoreMu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
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
		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
