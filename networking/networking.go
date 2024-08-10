package networking

import (
	"context"
	"fmt"
	"lamport-smr/helpers"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	peerstore "github.com/libp2p/go-libp2p/core/peer"
)

const PROTCOL_ID = "/smr/1.0.0"

// NodeCtx holds the host address, peer addresses, and connections.
type NodeCtx struct {
	pid     int
	host    host.Host
	streams []network.Stream
	sync.Mutex
}

// nodeCtx is the singleton instance of NodeCtx.
var nodeCtx *NodeCtx

// once is used to ensure that initCtx is only called once.
var once sync.Once

func initCtx(pid int, host host.Host) {
	once.Do(func() {
		nodeCtx = &NodeCtx{
			pid:     pid,
			host:    host,
			streams: make([]network.Stream, 0),
		}
	})
}

// StartHost starts the host and starts listening on the provided address
func StartHost(hostAddr string, pid int, wg *sync.WaitGroup) {
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
	initCtx(pid, host)
	nodeCtx.host.SetStreamHandler(PROTCOL_ID, handleStream)
	peerInfo := peerstore.AddrInfo{
		ID:    host.ID(),
		Addrs: host.Addrs(),
	}
	addrs, err := peerstore.AddrInfoToP2pAddrs(&peerInfo)
	if err != nil {
		log.Panicf("Error getting address info for peer %v: %v", pid, err)
	}
	fmt.Println("libp2p host address:", addrs[0])
	wg.Add(1)
	go waitForGracefulShutdown(wg)
}

// func removeConnection(conn net.Conn) {
// 	nodeCtx.Lock()
// 	defer nodeCtx.Unlock()

// 	for i, connection := range nodeCtx.connections {
// 		if connection.RemoteAddr().String() == conn.RemoteAddr().String() {
// 			nodeCtx.connections = append(nodeCtx.connections[:i], nodeCtx.connections[i+1:]...)
// 			break
// 		}
// 	}
// }

func handleStream(stream network.Stream) {
	for {

	}
}

// EstablishConnections establishes connections with the given peers.
func EstablishConnections(peers map[string]int) {
	if nodeCtx == nil {
		log.Panic("NodeCtx is not initialized")
	}
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

func waitForGracefulShutdown(wg *sync.WaitGroup) {
	defer wg.Done()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	fmt.Println("Received signal, shutting down...")
	if err := nodeCtx.host.Close(); err != nil {
		panic(err)
	}
}
