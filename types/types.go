package types

import (
	"fmt"
	"math/rand/v2"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
)

// NodeCtx holds the host address, peer addresses, and connections.
type NodeCtx struct {
	Host    host.Host
	Streams []network.Stream
	sync.Mutex
}

type CommandType int32

type MessageType int32

// Operation represents an operation in the state machine.
type Operation struct {
	Timestamp int32
	Command   CommandType
	Key       int32
	Value     int32 // Value is used only for SET commands (PUT & UPDATE)
}

// Acknowledgement represents an ack that means that the sender's timestamp was set to the given value
type Acknowledgement struct {
	Timestamp int32
}

type MessageContent interface{}

type Message struct {
	MsgType   MessageType
	Content   MessageContent
	SenderPid int
}

type SmCtx struct {
	Pid              int
	PeerPids         map[string]int
	StateMachine     map[int32]int32
	Logs             []*Operation
	Timestamps       []int32
	IncomingMessages chan *Message
	OutgoingMessages chan *Message
	StartSignal      chan struct{}
	Rng              *rand.Rand
	sync.Mutex
}

// FindRandomExistingKey returns a random key that exists in the state machine.
// It returns an error if the state machine is empty.
// Should be called under lock
func (smCtx *SmCtx) FindRandomExistingKey() (int32, error) {
	if len(smCtx.StateMachine) == 0 {
		return 0, fmt.Errorf("state machine is empty")
	}

	keys := make([]int32, 0, len(smCtx.StateMachine))

	for key := range smCtx.StateMachine {
		keys = append(keys, key)
	}

	randomKey := keys[smCtx.Rng.IntN(len(keys))]
	return randomKey, nil
}

// FindRandomNonExistingKey generates and returns a random key that does not exist in the state machine.
// Should be called under lock
func (smCtx *SmCtx) FindRandomNonExistingKey() int32 {
	for {
		randomKey := rand.Int32()
		if _, exists := smCtx.StateMachine[randomKey]; !exists {
			return randomKey
		}
	}
}

// SetOperation sets the content to an Operation, and resets Acknowledgement if it exists
func (m *Message) SetOperation(op Operation) {
	m.Content = op
}

// SetAcknowledgement sets the content to an Acknowledgement, and resets Operation if it exists
func (m *Message) SetAcknowledgement(ack Acknowledgement) {
	m.Content = ack
}
