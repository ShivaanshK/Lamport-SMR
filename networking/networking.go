package networking

import (
	"context"
	"io"
	"lamport-smr/helpers"
	sm "lamport-smr/state-machine"
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

// NodeCtx holds the host address, peer addresses, and connections.
type NodeCtx struct {
	host    host.Host
	streams []network.Stream
	sync.Mutex
}

// nodeCtx is the singleton instance of NodeCtx.
var nodeCtx *NodeCtx

// once is used to ensure that initCtx is only called once.
var once sync.Once

func initCtx(host host.Host) {
	once.Do(func() {
		nodeCtx = &NodeCtx{
			host:    host,
			streams: make([]network.Stream, 0),
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

	nodeCtx.host.SetStreamHandler(PROTCOL_ID, handleStream)
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
		if err := nodeCtx.host.Connect(context.Background(), *peerInfo); err != nil {
			log.Panicf("Failed to connect to peer %v: %v", peerAddr, err)
		}
		log.Printf("Successfully connected to %v", peerInfo.Addrs[0])
		stream, err := nodeCtx.host.NewStream(context.Background(), peerInfo.ID, PROTCOL_ID)
		if err != nil {
			log.Panicf("Error creating new stream with %v: %v", peerAddr, err)
		}
		log.Printf("Successfully created stream with %v", peerInfo.Addrs[0])
		nodeCtx.streams = append(nodeCtx.streams, stream)
	}
}

func handleOutgoingOps(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		op := <-sm.GlobalSmCtx.OutgoingOperations
		marshaledOp, err := sm.MarshalOperation(op)
		if err != nil {
			log.Panicf("Failed to marshal operation %v: %v", op, err)
		}
		for _, stream := range nodeCtx.streams {
			n, err := stream.Write(marshaledOp)
			if err != nil {
				log.Panicf("Failed to write operation to stream %v: %v", op, err)
			} else if n != len(marshaledOp) {
				log.Panicf("Failed to write entire operation to stream %v: %v", op, err)
			} else {
				log.Printf("Sent op %v to %v", op, stream.Conn().RemoteMultiaddr())
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
			op, err := sm.UnmarshalOperation(msg)
			if err != nil {
				log.Panicf("Error unmarshaling operation from %v: %v", stream.Conn().RemoteMultiaddr().String(), err)
			}
			sm.GlobalSmCtx.IncomingOperations <- op
		}
	}
}

func removeStream(stream network.Stream) {
	nodeCtx.Lock()
	defer nodeCtx.Unlock()

	for i, currSteam := range nodeCtx.streams {
		if stream.ID() == currSteam.ID() {
			nodeCtx.streams = append(nodeCtx.streams[:i], nodeCtx.streams[i+1:]...)
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
	if err := nodeCtx.host.Close(); err != nil {
		panic(err)
	}
}
