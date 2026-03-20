package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"

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
	n  *maelstrom.Node
	kv *maelstrom.KV
}

func NewState(n *maelstrom.Node) *State {
	return &State{
		n:  n,
		kv: maelstrom.NewLinKV(n),
	}
}

func makeLenKey(key string) string {
	return fmt.Sprintf("len:%s", key)
}

func makeOffsetKey(key string, offset int) string {
	return fmt.Sprintf("val:%s:%d", key, offset)
}

func makeCommitKey(key string) string {
	return fmt.Sprintf("commited:%s", key)
}

func (s *State) handleSend(msg maelstrom.Message) error {
	var body MessageSend
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx := context.Background()
	lenKey := makeLenKey(body.Key)

	offset := 0
	for {
		err := s.kv.CompareAndSwap(ctx, lenKey, offset, offset+1, true)
		if code := maelstrom.ErrorCode(err); code == maelstrom.PreconditionFailed {
			offset, err = s.kv.ReadInt(ctx, lenKey)
			if err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		offsetValueKey := makeOffsetKey(body.Key, offset)
		err = s.kv.Write(ctx, offsetValueKey, body.Msg)
		if err != nil {
			return err
		}
		break
	}

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

	ctx := context.Background()
	res := make(map[string][][]int)
	for k, offset := range body.Offsets {
		slog.Info("fetching len for Poll")
		l, err := s.kv.ReadInt(ctx, makeLenKey(k))
		code := maelstrom.ErrorCode(err)
		if err != nil && code != maelstrom.KeyDoesNotExist {
			slog.Error("failed fetch len for Poll")
			return err
		}

		messages := make([][]int, 0)
		for i := offset; i < l; i++ {
			slog.Info("fetching val for offset", slog.String("key", makeOffsetKey(k, i)))
			val, err := s.kv.ReadInt(ctx, makeOffsetKey(k, i))
			if err != nil {
				slog.Info("failed to fetch val for offset", slog.String("key", makeOffsetKey(k, i)))
				return err
			}
			messages = append(messages, []int{i, val})
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
	ctx := context.Background()

	for k, offset := range body.Offsets {
		err := s.kv.Write(ctx, makeCommitKey(k), offset)
		if err != nil {
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
		code := maelstrom.ErrorCode(err)
		if code == maelstrom.KeyDoesNotExist {
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
