package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	n    *maelstrom.Node
	kv   *maelstrom.KV
	mu   sync.Mutex
	logs map[string][]int
}

func NewState(n *maelstrom.Node) *State {
	return &State{
		n:    n,
		kv:   maelstrom.NewLinKV(n),
		logs: make(map[string][]int),
	}
}

func makeCommitKey(key string) string {
	return fmt.Sprintf("committed:%s", key)
}

func (s *State) ownerOf(key string) string {
	nodes := s.n.NodeIDs()
	h := 0
	for _, c := range key {
		h += int(c)
	}
	return nodes[h%len(nodes)]
}

func (s *State) handleSend(msg maelstrom.Message) error {
	var body MessageSend
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	owner := s.ownerOf(body.Key)
	if owner != s.n.ID() {
		resp, err := s.n.SyncRPC(context.Background(), owner, msg.Body)
		if err != nil {
			return err
		}
		return s.n.Reply(msg, resp.Body)
	}

	s.mu.Lock()
	s.logs[body.Key] = append(s.logs[body.Key], body.Msg)
	offset := len(s.logs[body.Key]) - 1
	s.mu.Unlock()

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

	// Group keys by owner
	byOwner := make(map[string]map[string]int)
	for k, offset := range body.Offsets {
		owner := s.ownerOf(k)
		if byOwner[owner] == nil {
			byOwner[owner] = make(map[string]int)
		}
		byOwner[owner][k] = offset
	}

	res := make(map[string][][]int)

	for owner, offsets := range byOwner {
		if owner == s.n.ID() {
			s.mu.Lock()
			for k, startOffset := range offsets {
				messages := make([][]int, 0)
				for i := startOffset; i < len(s.logs[k]); i++ {
					messages = append(messages, []int{i, s.logs[k][i]})
				}
				res[k] = messages
			}
			s.mu.Unlock()
		} else {
			pollBody, _ := json.Marshal(map[string]any{
				"type":    "poll",
				"offsets": offsets,
			})
			resp, err := s.n.SyncRPC(context.Background(), owner, json.RawMessage(pollBody))
			if err != nil {
				return err
			}
			var respBody struct {
				Msgs map[string][][]int `json:"msgs"`
			}
			if err := json.Unmarshal(resp.Body, &respBody); err != nil {
				return err
			}
			for k, msgs := range respBody.Msgs {
				res[k] = msgs
			}
		}
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
	ctx := context.Background()

	for k, offset := range body.Offsets {
		if err := s.kv.Write(ctx, makeCommitKey(k), offset); err != nil {
			return err
		}
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
	ctx := context.Background()

	res := make(map[string]int)
	for _, k := range body.Keys {
		val, err := s.kv.ReadInt(ctx, makeCommitKey(k))
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			continue
		}
		if err != nil {
			return err
		}
		res[k] = val
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
