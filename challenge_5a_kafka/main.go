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

type MessageSend struct {
	BaseMessage
	Key string `json:"key"`
	Msg int    `json:"msg"`
}

type MessagePoll struct {
	BaseMessage
	Offsets map[string]int `json:"offsets"`
}

type MessageCommitOffsets struct {
	BaseMessage
	Offsets map[string]int `json:"offsets"`
}

type MessageListCommitedOffsets struct {
	BaseMessage
	Keys []string `json:"keys"`
}

type State struct {
	n               *maelstrom.Node
	mu              sync.Mutex
	logs            map[string][]int
	commitedOffsets map[string]int
}

func NewState(n *maelstrom.Node) *State {
	return &State{
		n:               n,
		logs:            make(map[string][]int),
		commitedOffsets: make(map[string]int),
	}
}

func (s *State) handleSend(msg maelstrom.Message) error {
	var body MessageSend
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logs[body.Key] = append(s.logs[body.Key], body.Msg)
	offset := len(s.logs[body.Key]) - 1

	return s.n.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

func (s *State) handlePoll(msg maelstrom.Message) error {
	var body MessagePoll
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[string][][]int)
	for k, offset := range body.Offsets {
		messages := make([][]int, 0)
		for i := offset; i < len(s.logs[k]); i++ {
			messages = append(messages, []int{i, s.logs[k][i]})
		}
		res[k] = messages
	}

	return s.n.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": res,
	})
}

func (s *State) handleCommitOffset(msg maelstrom.Message) error {
	var body MessageCommitOffsets
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, offset := range body.Offsets {
		s.commitedOffsets[k] = offset
	}

	return s.n.Reply(msg, map[string]any{
		"type": "commit_offsets_ok",
	})
}

func (s *State) handleListCommitedOffsets(msg maelstrom.Message) error {
	var body MessageListCommitedOffsets
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[string]int)
	for _, k := range body.Keys {
		res[k] = s.commitedOffsets[k]
	}

	return s.n.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": res,
	})
}

func main() {
	n := maelstrom.NewNode()
	state := NewState(n)

	n.Handle("send", state.handleSend)
	n.Handle("poll", state.handlePoll)
	n.Handle("commit_offsets", state.handleCommitOffset)
	n.Handle("list_committed_offsets", state.handleListCommitedOffsets)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
