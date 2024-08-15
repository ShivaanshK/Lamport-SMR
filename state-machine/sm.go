package sm

import (
	"lamport-smr/types"
	"log"
	"math/rand/v2"
	"os"
	"sync"
	"time"
)

var GlobalSmCtx *types.SmCtx

// once is used to ensure that initCtx is only called once.
var once sync.Once

// NewSmCtx creates a new state machine context with initialized fields.
func InitGlobalSmCtx(pid int32, peerPids map[string]int, wg *sync.WaitGroup) {
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
		case types.OP:
			opContent, ok := incomingMsg.Content.(types.Operation)
			if !ok {
				log.Panicf("Couldn't find operation in message")
			}
			op := &opContent
			log.Printf("Received Operation: %v", op)
			GlobalSmCtx.AppendLog(op)                                                        // Add new op to logs
			GlobalSmCtx.TimestampsMutex.Lock()                                               // Reads and updates on timestamps array under lock
			GlobalSmCtx.Timestamps[pid] = max(op.Timestamp, GlobalSmCtx.Timestamps[pid]) + 1 // Update local lamport clock to max of op and local + 1 (for receive)
			GlobalSmCtx.Timestamps[op.SenderPid] = op.Timestamp                              // Update sending process's lamport clock to what they sent
			GlobalSmCtx.TimestampsMutex.Unlock()
			// Create and send an acknowledgement
			ack := types.Acknowledgement{
				Timestamp: GlobalSmCtx.Timestamps[pid],
				SenderPid: op.SenderPid,
			}
			msg := &types.Message{
				MsgType: types.ACK,
			}
			msg.SetAcknowledgement(ack)
			GlobalSmCtx.OutgoingMessages <- msg
		case types.ACK:
			ackContent, ok := incomingMsg.Content.(types.Acknowledgement)
			if !ok {
				log.Panicf("Couldn't find acknowledgment in message")
			}
			GlobalSmCtx.UpdateTimestamp(int(ackContent.SenderPid), ackContent.Timestamp) // Update the acknowledged lamport clock for the given process
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
				if int32(index) == GlobalSmCtx.Pid {
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
		timeToWait := r.IntN(10)
		log.Printf("---WAITING %v SECONDS---", timeToWait)
		time.Sleep(time.Duration(timeToWait) * time.Second)

		// Local Event
		GlobalSmCtx.TimestampsMutex.Lock()
		var command types.CommandType
		if len(GlobalSmCtx.StateMachine) == 0 {
			command = types.PUT
		} else {
			command = types.CommandType(r.Int32N(4))
		}
		var key int32 = 0
		var value int32 = 0
		if command == types.GET || command == types.DELETE {
			existentKey, err := GlobalSmCtx.FindRandomExistingKey()
			if err != nil {
				log.Panicf("Couldnt find a key that exists: %v", err)
			}
			key = existentKey
		} else {
			if command == types.PUT {
				key = GlobalSmCtx.FindRandomNonExistingKey()
			} else {
				existentKey, err := GlobalSmCtx.FindRandomExistingKey()
				if err != nil {
					log.Panicf("Couldnt find a key that exists: %v", err)
				}
				key = existentKey
			}
			value = r.Int32()
		}
		GlobalSmCtx.Timestamps[pid] = GlobalSmCtx.Timestamps[pid] + 1
		op := createOperation(command, GlobalSmCtx.Pid, GlobalSmCtx.Timestamps[pid], key, value)
		GlobalSmCtx.TimestampsMutex.Unlock()
		log.Printf("CLIENT REQUEST: %v", op)
		GlobalSmCtx.AppendLog(&op)
		msg := &types.Message{
			MsgType: types.OP,
		}
		msg.SetOperation(op)
		GlobalSmCtx.OutgoingMessages <- msg
	}
}

func createOperation(command types.CommandType, pid, timestamp, key, value int32) types.Operation {
	return types.Operation{
		SenderPid: pid,
		Timestamp: timestamp,
		Command:   command,
		Key:       key,
		Value:     value,
	}
}
