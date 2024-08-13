package networking

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"lamport-smr/helpers"
	sm "lamport-smr/state-machine"
	"lamport-smr/types"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

const PROTCOL_ID = "/smr/1.0.0"

// nodeCtx is the singleton instance of NodeCtx.
var nodeCtx *types.NodeCtx

// once is used to ensure that initCtx is only called once.
var once sync.Once

func initCtx(host host.Host) {
	once.Do(func() {
		nodeCtx = &types.NodeCtx{
			Host:    host,
			Streams: make([]network.Stream, 0),
		}
	})
}

// StartHost starts the host and starts listening on the provided address
func StartHost(hostAddr string, wg *sync.WaitGroup) {
	if sm.GlobalSmCtx == nil {
		log.Panic("GlobalSmCtx is not initialized")
	}

	pid := sm.GlobalSmCtx.Pid
	priv, err := helpers.GetKey(pid)
	if err != nil {
		log.Panicf("Error getting private key for peer %v: %v", pid, err)
	}

	multiAddr, err := helpers.ParseMultiaddress(hostAddr)
	if err != nil {
		log.Panicf("Error parsing multiaddress for %v: %v", pid, err)
	}
	host, err := libp2p.New(
		// Use the keypair
		libp2p.Identity(priv),
		// Multiple listen addresses
		libp2p.ListenAddrStrings(multiAddr),
	)
	if err != nil {
		log.Panicf("Error starting the host: %v", err)
	}

	initCtx(host)

	nodeCtx.Host.SetStreamHandler(PROTCOL_ID, handleStream)
	log.Println("---SUCCESSFULLY INITIALIZED HOST---")

	wg.Add(2)
	go handleOutgoingOps(wg)
	go waitForShutdownSignal(wg)
}

// EstablishConnections establishes connections with the given peers.
func EstablishConnections() {
	if nodeCtx == nil {
		log.Panic("NodeCtx is not initialized")
	}
	if sm.GlobalSmCtx == nil {
		log.Panic("GlobalSmCtx is not initialized")
	}

	peers := sm.GlobalSmCtx.PeerPids
	nodeCtx.Lock()
	defer nodeCtx.Unlock()

	for peerAddr := range peers {
		peerInfo, err := peer.AddrInfoFromString(peerAddr)
		if err != nil {
			log.Panicf("Error getting multiaddr info: %v", err)
		}
		if err := nodeCtx.Host.Connect(context.Background(), *peerInfo); err != nil {
			log.Panicf("Failed to connect to peer %v: %v", peerAddr, err)
		}
		log.Printf("Successfully connected to %v", peerInfo.Addrs[0])
		stream, err := nodeCtx.Host.NewStream(context.Background(), peerInfo.ID, PROTCOL_ID)
		if err != nil {
			log.Panicf("Error creating new stream with %v: %v", peerAddr, err)
		}
		log.Printf("Successfully created stream with %v", peerInfo.Addrs[0])
		nodeCtx.Streams = append(nodeCtx.Streams, stream)
	}
}

func handleOutgoingOps(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		msg := <-sm.GlobalSmCtx.OutgoingMessages
		marshaledMsg, err := MarshalMessage(msg)
		if err != nil {
			log.Panicf("Failed to marshal message: %v", err)
		}
		for _, stream := range nodeCtx.Streams {
			n, err := stream.Write(marshaledMsg)
			if err != nil {
				log.Panicf("Failed to write operation to stream: %v", err)
			} else if n != len(marshaledMsg) {
				log.Panicf("Failed to write entire operation to stream: %v", err)
			} else {
				log.Printf("Sent message of type %v to %v", msg.MsgType, stream.Conn().RemoteMultiaddr())
			}
		}
	}
}

func handleStream(stream network.Stream) {
	defer func() {
		removeStream(stream)
		stream.Close()
	}()

	log.Println("Stream opened to ", stream.Conn().RemoteMultiaddr().String())

	buffer := make([]byte, 1024) // Buffer to hold incoming data

	for {
		n, err := stream.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Panicf("Error reading from stream with %v: %v", stream.Conn().RemoteMultiaddr().String(), err)
			}
			break
		}

		if n > 0 {
			msg := buffer[:n]
			unmarshaledMsg, err := UnmarshalMessage(msg)
			if err != nil {
				log.Panicf("Error unmarshaling operation from %v: %v", stream.Conn().RemoteMultiaddr().String(), err)
			}
			sm.GlobalSmCtx.IncomingMessages <- unmarshaledMsg
		}
	}
}

func removeStream(stream network.Stream) {
	nodeCtx.Lock()
	defer nodeCtx.Unlock()

	for i, currSteam := range nodeCtx.Streams {
		if stream.ID() == currSteam.ID() {
			nodeCtx.Streams = append(nodeCtx.Streams[:i], nodeCtx.Streams[i+1:]...)
			break
		}
	}
}

func waitForShutdownSignal(wg *sync.WaitGroup) {
	defer wg.Done()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Println("Received signal, shutting down...")
	if err := nodeCtx.Host.Close(); err != nil {
		panic(err)
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
	case types.OP:
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

	case types.ACK:
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
	case types.OP:
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

	case types.ACK:
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
