package sm

import "sync"

// Command represents an operation in the state machine.
type Command struct {
	Timestamp int
	Command   int
	Key       int
	Value     int // Value is used only for SET commands (PUT & UPDATE)
}

type SmCtx struct {
	StateMachine     map[int]int
	Logs             []Command
	Timestamps       [][]int
	IncomingCommands chan Command
	OutgoingCommands chan Command
}

var GlobalSmCtx *SmCtx

// once is used to ensure that initCtx is only called once.
var once sync.Once

// NewSmCtx creates a new state machine context with initialized fields.
func NewSm() {
	once.Do(func() {
		GlobalSmCtx = &SmCtx{
			StateMachine:     make(map[int]int),
			Logs:             make([]Command, 0),
			Timestamps:       make([][]int, 0),
			IncomingCommands: make(chan Command),
			OutgoingCommands: make(chan Command),
		}
	})
}
