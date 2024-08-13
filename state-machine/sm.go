package sm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"lamport-smr/types"
	"log"
	"math/rand/v2"
	"os"
	"sync"
	"time"
)

// Command Types
const (
	GET types.CommandType = iota
	PUT
	DELETE
	UPDATE
)

// Message Types
const (
	OP types.MessageType = iota
	ACK
)

var GlobalSmCtx *types.SmCtx

// once is used to ensure that initCtx is only called once.
var once sync.Once

// NewSmCtx creates a new state machine context with initialized fields.
func InitGlobalSmCtx(pid int, peerPids map[string]int, wg *sync.WaitGroup) {
	once.Do(func() {
		numProcesses := 1 + len(peerPids)
		GlobalSmCtx = &types.SmCtx{
			Pid:              pid,
			PeerPids:         peerPids,
			StateMachine:     make(map[int32]int32),
			Logs:             make([]*types.Operation, 0),
			Timestamps:       make([]int32, numProcesses),
			IncomingMessages: make(chan *types.Message),
			OutgoingMessages: make(chan *types.Message),
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
		switch incomingMsg.MsgType {
		case OP:
			opContent, ok := incomingMsg.Content.(types.Operation)
			if !ok {
				log.Panicf("Couldn't find operation in message")
			}
			op := &opContent
			log.Printf("Received Operation: %v", op)
			GlobalSmCtx.AppendLog(op)                                                        // Add new op to logs
			GlobalSmCtx.TimestampsMutex.Lock()                                               // Reads and updates on timestamps array under lock
			GlobalSmCtx.Timestamps[pid] = max(op.Timestamp, GlobalSmCtx.Timestamps[pid]) + 1 // Update local lamport clock to max of op and local + 1 (for receive)
			GlobalSmCtx.Timestamps[incomingMsg.SenderPid] = op.Timestamp                     // Update sending process's lamport clock to what they sent
			GlobalSmCtx.TimestampsMutex.Unlock()
			// Create and send an acknowledgement
			ack := types.Acknowledgement{
				Timestamp: GlobalSmCtx.Timestamps[pid],
			}
			msg := &types.Message{
				MsgType:   ACK,
				SenderPid: GlobalSmCtx.Pid,
			}
			msg.SetAcknowledgement(ack)
			GlobalSmCtx.OutgoingMessages <- msg
		case ACK:
			ackContent, ok := incomingMsg.Content.(types.Acknowledgement)
			if !ok {
				log.Panicf("Couldn't find acknowledgment in message")
			}
			GlobalSmCtx.UpdateTimestamp(incomingMsg.SenderPid, ackContent.Timestamp) // Update the acknowledged lamport clock for the given process
		}
	}
}

func constantCheck(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		op, exists := GlobalSmCtx.PeekLog()
		if exists {
			canApply := true
			GlobalSmCtx.TimestampsMutex.Lock()
			for index, timestamp := range GlobalSmCtx.Timestamps {
				if index == GlobalSmCtx.Pid {
					continue
				}
				if op.Timestamp >= timestamp {
					canApply = false
					break
				}
			}
			GlobalSmCtx.TimestampsMutex.Unlock()
			if canApply {
				log.Printf("APPLYING OP: %v", op)
				GlobalSmCtx.Apply(op)
				GlobalSmCtx.PopLog()
			}
		}
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
		command := types.CommandType(r.Int32N(4))
		var key int32 = 0
		var value int32 = 0
		if command == GET || command == DELETE {
			existentKey, err := GlobalSmCtx.FindRandomExistingKey()
			if err != nil {
				log.Panicf("Couldnt find a key that exists: %v", err)
			}
			key = existentKey
		} else {
			nonExistentKey, err := GlobalSmCtx.FindRandomExistingKey()
			if err != nil {
				log.Panicf("Couldnt find a key that exists: %v", err)
			}
			key = nonExistentKey
			value = r.Int32N(1000)
		}
		GlobalSmCtx.IncrementTimestamp(pid)
		op := createOperation(command, GlobalSmCtx.GetTimestamp(pid), key, value)
		log.Printf("CLIENT REQUEST: %v", command)
		msg := &types.Message{
			MsgType:   OP,
			SenderPid: GlobalSmCtx.Pid,
		}
		msg.SetOperation(op)
		GlobalSmCtx.OutgoingMessages <- msg
	}
}

func createOperation(command types.CommandType, timestamp, key, value int32) types.Operation {
	return types.Operation{
		Timestamp: timestamp,
		Command:   command,
		Key:       key,
		Value:     value,
	}
}

// MarshalMessage converts a Message struct into a byte slice and returns an error if it fails.
func MarshalMessage(msg *types.Message) ([]byte, error) {
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
		op, ok := msg.Content.(types.Operation)
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
		ack, ok := msg.Content.(types.Acknowledgement)
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
func UnmarshalMessage(data []byte) (*types.Message, error) {
	buf := bytes.NewReader(data)
	msg := &types.Message{}

	// Read the MsgType and SenderPid
	var msgType, senderPid int32
	if err := binary.Read(buf, binary.LittleEndian, &msgType); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &senderPid); err != nil {
		return nil, err
	}

	msg.MsgType = types.MessageType(msgType)
	msg.SenderPid = int(senderPid)

	// Read the content based on the MsgType
	switch msg.MsgType {
	case OP:
		var op types.Operation
		if err := binary.Read(buf, binary.LittleEndian, &op.Timestamp); err != nil {
			return nil, err
		}
		var command int32
		if err := binary.Read(buf, binary.LittleEndian, &command); err != nil {
			return nil, err
		}
		op.Command = types.CommandType(command)
		if err := binary.Read(buf, binary.LittleEndian, &op.Key); err != nil {
			return nil, err
		}
		if err := binary.Read(buf, binary.LittleEndian, &op.Value); err != nil {
			return nil, err
		}
		msg.Content = op

	case ACK:
		var ack types.Acknowledgement
		if err := binary.Read(buf, binary.LittleEndian, &ack.Timestamp); err != nil {
			return nil, err
		}
		msg.Content = ack

	default:
		return nil, errors.New("unknown message type")
	}

	return msg, nil
}
