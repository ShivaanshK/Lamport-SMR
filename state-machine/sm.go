package sm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"
)

const (
	GET int32 = iota
	PUT
	DELETE
	UPDATE
)

// Operation represents an operation in the state machine.
type Operation struct {
	Timestamp int32
	Command   int32
	Key       int32
	Value     int32 // Value is used only for SET commands (PUT & UPDATE)
}

type SmCtx struct {
	Pid                int
	PeerPids           map[string]int
	StateMachine       map[int32]int32
	Logs               []*Operation
	Timestamps         []int32
	IncomingOperations chan *Operation
	OutgoingOperations chan *Operation
	StartSignal        chan struct{}
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
			Pid:                pid,
			PeerPids:           peerPids,
			StateMachine:       make(map[int32]int32),
			Logs:               make([]*Operation, 0),
			Timestamps:         make([]int32, numProcesses),
			IncomingOperations: make(chan *Operation),
			OutgoingOperations: make(chan *Operation),
			StartSignal:        make(chan struct{}),
		}
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

	for {
		op := <-GlobalSmCtx.IncomingOperations
		log.Printf("Received Operation: %v", op)
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

	// Simulate a client making intermittent requests
	for {
		timeToExecute := rand.IntN(10)
		time.Sleep(time.Duration(timeToExecute) * time.Second)

		command := rand.Int32N(4)
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
			value = rand.Int32N(1000)
		}
		GlobalSmCtx.Timestamps[pid] = GlobalSmCtx.Timestamps[pid] + 1
		op := createOperation(GlobalSmCtx.Timestamps[pid], command, key, value)
		log.Printf("CLIENT REQUEST: %v", command)
		GlobalSmCtx.OutgoingOperations <- op
	}
}

func createOperation(timestamp, command, key, value int32) *Operation {
	return &Operation{
		Timestamp: timestamp,
		Command:   command,
		Key:       key,
		Value:     value,
	}
}

// MarshalOperation converts an Operation struct into a byte slice and returns an error if it fails.
func MarshalOperation(op *Operation) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write the fields to the buffer in a specific order as int32
	if err := binary.Write(buf, binary.LittleEndian, int32(op.Timestamp)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, int32(op.Command)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, int32(op.Key)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, int32(op.Value)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// UnmarshalOperation converts a byte slice back into an Operation struct and returns an error if it fails.
func UnmarshalOperation(data []byte) (*Operation, error) {
	if len(data) != 16 {
		return nil, errors.New("invalid data length for unmarshaling Operation")
	}

	buf := bytes.NewReader(data)
	op := &Operation{}

	var timestamp, command, key, value int32

	// Read the fields from the buffer as int32 in the same order they were written
	if err := binary.Read(buf, binary.LittleEndian, &timestamp); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &command); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &key); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
		return nil, err
	}

	// Assign the values to the operation
	op.Timestamp = int32(timestamp)
	op.Command = int32(command)
	op.Key = int32(key)
	op.Value = int32(value)

	return op, nil
}

// FindRandomExistingKey returns a random key that exists in the state machine.
// It returns an error if the state machine is empty.
func (smCtx *SmCtx) findRandomExistingKey() (int32, error) {
	smCtx.Lock()
	defer smCtx.Unlock()

	if len(smCtx.StateMachine) == 0 {
		return 0, fmt.Errorf("state machine is empty")
	}

	keys := make([]int32, 0, len(smCtx.StateMachine))

	for key := range smCtx.StateMachine {
		keys = append(keys, key)
	}

	randomKey := keys[rand.IntN(len(keys))]
	return randomKey, nil
}

// FindRandomNonExistingKey generates and returns a random key that does not exist in the state machine.
func (smCtx *SmCtx) FindRandomNonExistingKey() int32 {
	smCtx.Lock()
	defer smCtx.Unlock()

	for {
		randomKey := rand.Int32()

		if _, exists := smCtx.StateMachine[randomKey]; !exists {
			return randomKey
		}
	}
}
