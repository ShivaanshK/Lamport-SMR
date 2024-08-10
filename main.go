package main

import (
	"flag"
	"lamport-smr/helpers"
	"lamport-smr/networking"
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

	log.Printf("%v", serverAddr)
	log.Printf("%v", peers)

	var wg sync.WaitGroup

	networking.StartHost(serverAddr, *pid, &wg)
	time.Sleep(3 * time.Second)
	networking.EstablishConnections(peers)

	wg.Wait()
}
