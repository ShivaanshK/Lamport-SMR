package sm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"sync"
	"time"
)

type CommandType int32

// Command Types
const (
	GET CommandType = iota
	PUT
	DELETE
	UPDATE
)

type MessageType int32

// Message Types
const (
	OP MessageType = iota
	ACK
)

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

var GlobalSmCtx *SmCtx

// once is used to ensure that initCtx is only called once.
var once sync.Once

// NewSmCtx creates a new state machine context with initialized fields.
func InitGlobalSmCtx(pid int, peerPids map[string]int, wg *sync.WaitGroup) {
	once.Do(func() {
		numProcesses := 1 + len(peerPids)
		GlobalSmCtx = &SmCtx{
			Pid:              pid,
			PeerPids:         peerPids,
			StateMachine:     make(map[int32]int32),
			Logs:             make([]*Operation, 0),
			Timestamps:       make([]int32, numProcesses),
			IncomingMessages: make(chan *Message),
			OutgoingMessages: make(chan *Message),
			StartSignal:      make(chan struct{}),
		}
		// Init RNG with a random seed
		GlobalSmCtx.Rng = rand.New(rand.NewPCG(uint64(os.Getpid()), uint64(pid)))
		// Initialize it with 1 value
		GlobalSmCtx.StateMachine[0] = 0
		wg.Add(3)
		go onReceive(wg)
		go constantCheck(wg)
		go simulateExecution(wg)
	})
}

func onReceive(wg *sync.WaitGroup) {
	defer wg.Done()

	log.Println("---READY TO RECEIVE OPERATIONS---")
	pid := GlobalSmCtx.Pid

	for {
		incomingMsg := <-GlobalSmCtx.IncomingMessages
		GlobalSmCtx.Lock()
		switch incomingMsg.MsgType {
		case OP:
			opContent, ok := incomingMsg.Content.(Operation)
			if !ok {
				log.Panicf("Couldn't find operation in message")
			}
			op := &opContent
			log.Printf("Received Operation: %v", op)
			GlobalSmCtx.Logs = append(GlobalSmCtx.Logs, op)                                  // Add new op to logs
			GlobalSmCtx.Timestamps[pid] = max(op.Timestamp, GlobalSmCtx.Timestamps[pid]) + 1 // Update local lamport clock to max of op and local + 1 (for receive)
			GlobalSmCtx.Timestamps[incomingMsg.SenderPid] = op.Timestamp                     // Update sending process's lamport clock to what they sent
			GlobalSmCtx.Unlock()
			// Create and send an acknowledgement
			ack := Acknowledgement{
				Timestamp: GlobalSmCtx.Timestamps[pid],
			}
			msg := &Message{
				MsgType:   ACK,
				SenderPid: GlobalSmCtx.Pid,
			}
			msg.SetAcknowledgement(ack)
			GlobalSmCtx.OutgoingMessages <- msg
		case ACK:
			ackContent, ok := incomingMsg.Content.(Acknowledgement)
			if !ok {
				log.Panicf("Couldn't find acknowledgment in message")
			}
			GlobalSmCtx.Timestamps[incomingMsg.SenderPid] = ackContent.Timestamp // Update the acknowledged lamport clock for the given process
			GlobalSmCtx.Unlock()
		default:
			GlobalSmCtx.Unlock()
		}
	}
}

func constantCheck(wg *sync.WaitGroup) {
	defer wg.Done()

	for {

	}
}

func simulateExecution(wg *sync.WaitGroup) {
	defer wg.Done()
	<-GlobalSmCtx.StartSignal

	log.Println("---STARTED EXECUTION---")
	pid := GlobalSmCtx.Pid
	r := GlobalSmCtx.Rng

	// Simulate a client making intermittent requests
	for {
		timeToExecute := r.IntN(10)
		time.Sleep(time.Duration(timeToExecute) * time.Second)

		// Local event
		GlobalSmCtx.Lock()
		command := CommandType(r.Int32N(4))
		var key int32 = 0
		var value int32 = 0
		if command == GET || command == DELETE {
			existentKey, err := GlobalSmCtx.findRandomExistingKey()
			if err != nil {
				log.Panicf("Couldnt find a key that exists: %v", err)
			}
			key = existentKey
		} else {
			nonExistentKey, err := GlobalSmCtx.findRandomExistingKey()
			if err != nil {
				log.Panicf("Couldnt find a key that exists: %v", err)
			}
			key = nonExistentKey
			value = r.Int32N(1000)
		}
		GlobalSmCtx.Timestamps[pid] = GlobalSmCtx.Timestamps[pid] + 1
		GlobalSmCtx.Unlock()
		op := createOperation(command, GlobalSmCtx.Timestamps[pid], key, value)
		log.Printf("CLIENT REQUEST: %v", command)
		msg := &Message{
			MsgType:   OP,
			SenderPid: GlobalSmCtx.Pid,
		}
		msg.SetOperation(op)
		GlobalSmCtx.OutgoingMessages <- msg
	}
}

func createOperation(command CommandType, timestamp, key, value int32) Operation {
	return Operation{
		Timestamp: timestamp,
		Command:   command,
		Key:       key,
		Value:     value,
	}
}

// MarshalMessage converts a Message struct into a byte slice and returns an error if it fails.
func MarshalMessage(msg *Message) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write the MsgType and SenderPid
	if err := binary.Write(buf, binary.LittleEndian, int32(msg.MsgType)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, int32(msg.SenderPid)); err != nil {
		return nil, err
	}

	// Write the content based on the MsgType
	switch msg.MsgType {
	case OP:
		op, ok := msg.Content.(Operation)
		if !ok {
			return nil, errors.New("invalid content type for OP message")
		}
		if err := binary.Write(buf, binary.LittleEndian, op.Timestamp); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, int32(op.Command)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, op.Key); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, op.Value); err != nil {
			return nil, err
		}

	case ACK:
		ack, ok := msg.Content.(Acknowledgement)
		if !ok {
			return nil, errors.New("invalid content type for ACK message")
		}
		if err := binary.Write(buf, binary.LittleEndian, ack.Timestamp); err != nil {
			return nil, err
		}

	default:
		return nil, errors.New("unknown message type")
	}

	return buf.Bytes(), nil
}

// UnmarshalMessage converts a byte slice back into a Message struct and returns an error if it fails.
func UnmarshalMessage(data []byte) (*Message, error) {
	buf := bytes.NewReader(data)
	msg := &Message{}

	// Read the MsgType and SenderPid
	var msgType, senderPid int32
	if err := binary.Read(buf, binary.LittleEndian, &msgType); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &senderPid); err != nil {
		return nil, err
	}

	msg.MsgType = MessageType(msgType)
	msg.SenderPid = int(senderPid)

	// Read the content based on the MsgType
	switch msg.MsgType {
	case OP:
		var op Operation
		if err := binary.Read(buf, binary.LittleEndian, &op.Timestamp); err != nil {
			return nil, err
		}
		var command int32
		if err := binary.Read(buf, binary.LittleEndian, &command); err != nil {
			return nil, err
		}
		op.Command = CommandType(command)
		if err := binary.Read(buf, binary.LittleEndian, &op.Key); err != nil {
			return nil, err
		}
		if err := binary.Read(buf, binary.LittleEndian, &op.Value); err != nil {
			return nil, err
		}
		msg.Content = op

	case ACK:
		var ack Acknowledgement
		if err := binary.Read(buf, binary.LittleEndian, &ack.Timestamp); err != nil {
			return nil, err
		}
		msg.Content = ack

	default:
		return nil, errors.New("unknown message type")
	}

	return msg, nil
}

// FindRandomExistingKey returns a random key that exists in the state machine.
// It returns an error if the state machine is empty.
// Should be called under lock
func (smCtx *SmCtx) findRandomExistingKey() (int32, error) {
	if len(smCtx.StateMachine) == 0 {
		return 0, fmt.Errorf("state machine is empty")
	}

	keys := make([]int32, 0, len(smCtx.StateMachine))

	for key := range smCtx.StateMachine {
		keys = append(keys, key)
	}

	randomKey := keys[GlobalSmCtx.Rng.IntN(len(keys))]
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
