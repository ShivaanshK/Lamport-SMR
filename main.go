package main

import (
	"flag"
	"lamport-smr/helpers"
	"lamport-smr/networking"
	sm "lamport-smr/state-machine"
	"log"
	"sync"
	"time"
)

func main() {
	configPath := flag.String("config_path", "./config.json", "Path to config file")
	pid := flag.Int("pid", 0, "Process ID")
	flag.Parse()

	serverAddr, peers, err := helpers.GetHostAndMapping(*configPath, *pid)
	if err != nil {
		log.Panicf("Error parsing config file: %v", err)
	}
	var wg sync.WaitGroup

	sm.InitGlobalSmCtx(*pid, peers, &wg)

	networking.StartHost(serverAddr, &wg)
	time.Sleep(3 * time.Second) // Give time to all peers to start their hosts
	networking.EstablishConnections()

	close(sm.GlobalSmCtx.StartSignal) // Send start signal after network is setup

	wg.Wait()
}
