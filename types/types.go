package types

import (
	"fmt"
	"log"
	"math/rand/v2"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
)

type CommandType int32

type MessageType int32

// Command Types
const (
	GET CommandType = iota
	PUT
	DELETE
	UPDATE
)

// Message Types
const (
	OP MessageType = iota
	ACK
)

// NodeCtx holds the host address, peer addresses, and connections.
type NodeCtx struct {
	Host    host.Host
	Streams []network.Stream
	sync.Mutex
}

// Operation represents an operation in the state machine.
type Operation struct {
	SenderPid int32
	Timestamp int32
	Command   CommandType
	Key       int32
	Value     int32 // Value is used only for SET commands (PUT & UPDATE)
}

// Acknowledgement represents an ack that means that the sender's timestamp was set to the given value
type Acknowledgement struct {
	SenderPid int32
	Timestamp int32
}

type MessageContent interface{}

type Message struct {
	MsgType MessageType
	Content MessageContent
}

type SmCtx struct {
	Pid              int32
	PeerPids         map[string]int
	StateMachine     map[int32]int32
	SmMutex          sync.Mutex
	Logs             []*Operation
	LogsMutex        sync.Mutex
	Timestamps       []int32
	TimestampsMutex  sync.Mutex
	IncomingMessages chan *Message
	OutgoingMessages chan *Message
	StartSignal      chan struct{}
	Rng              *rand.Rand
}

// FindRandomExistingKey returns a random key that exists in the state machine.
// It returns an error if the state machine is empty.
// Should be called under lock
func (smCtx *SmCtx) FindRandomExistingKey() (int32, error) {
	smCtx.SmMutex.Lock()
	defer smCtx.SmMutex.Unlock()

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
	smCtx.SmMutex.Lock()
	defer smCtx.SmMutex.Unlock()

	for {
		randomKey := rand.Int32()
		if _, exists := smCtx.StateMachine[randomKey]; !exists {
			return randomKey
		}
	}
}

func (smCtx *SmCtx) PeekLog() (*Operation, bool) {
	smCtx.LogsMutex.Lock()
	defer smCtx.LogsMutex.Unlock()

	if len(smCtx.Logs) > 0 {
		return smCtx.Logs[0], true
	} else {
		return nil, false
	}
}

func (smCtx *SmCtx) PopLog() {
	smCtx.LogsMutex.Lock()
	defer smCtx.LogsMutex.Unlock()

	smCtx.Logs = smCtx.Logs[1:]
}

func (smCtx *SmCtx) AppendLog(op *Operation) {
	smCtx.LogsMutex.Lock()
	defer smCtx.LogsMutex.Unlock()
	// Find the appropriate position to insert the new operation
	for index, curr_op := range smCtx.Logs {
		if curr_op.Timestamp > op.Timestamp {
			// Insert op before curr_op
			smCtx.Logs = append(smCtx.Logs[:index], append([]*Operation{op}, smCtx.Logs[index:]...)...)
			return
		} else if curr_op.Timestamp == op.Timestamp {
			// Tie-breaking using sender's PID
			if op.SenderPid < curr_op.SenderPid {
				smCtx.Logs = append(smCtx.Logs[:index], append([]*Operation{op}, smCtx.Logs[index:]...)...)
				return
			}
		}
	}
	smCtx.Logs = append(smCtx.Logs, op)
}

func (smCtx *SmCtx) UpdateTimestamp(pid int, timestamp int32) {
	smCtx.TimestampsMutex.Lock()
	defer smCtx.TimestampsMutex.Unlock()

	smCtx.Timestamps[pid] = int32(timestamp)
}

func (smCtx *SmCtx) IncrementTimestamp(pid int) {
	smCtx.TimestampsMutex.Lock()
	defer smCtx.TimestampsMutex.Unlock()

	smCtx.Timestamps[pid] = smCtx.Timestamps[pid] + 1
}

func (smCtx *SmCtx) GetTimestamp(pid int) int32 {
	smCtx.TimestampsMutex.Lock()
	defer smCtx.TimestampsMutex.Unlock()

	return smCtx.Timestamps[pid]
}

func (smCtx *SmCtx) Apply(op *Operation) {
	smCtx.SmMutex.Lock()
	defer smCtx.SmMutex.Unlock()

	// Implement
	switch op.Command {
	case GET:
		// In reality would involve returning value to client or sender
		log.Printf("GET returned %v from key %v", smCtx.StateMachine[op.Key], op.Key)
	case PUT:
		log.Printf("PUT %v in key %v", op.Value, op.Key)
		smCtx.StateMachine[op.Key] = op.Value
	case DELETE:
		log.Printf("DELETED key %v with value %v", op.Key, op.Value)
		delete(smCtx.StateMachine, op.Key)
	case UPDATE:
		log.Printf("UPDATED key %v with value %v", op.Key, op.Value)
		smCtx.StateMachine[op.Key] = op.Value
	default:
		log.Panicf("Invalid command: %v", op.Command)
	}

	log.Printf("RESULTING STATE MACHINE:\n%v", smCtx.StateMachine)
}

// SetOperation sets the content to an Operation, and resets Acknowledgement if it exists
func (m *Message) SetOperation(op Operation) {
	m.Content = op
}

// SetAcknowledgement sets the content to an Acknowledgement, and resets Operation if it exists
func (m *Message) SetAcknowledgement(ack Acknowledgement) {
	m.Content = ack
}
