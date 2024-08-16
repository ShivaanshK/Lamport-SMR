# Lamport-SMR

## Overview

This repository implements Lamport's State Machine Replication (SMR) algorithm using lamport (logical) clocks for consistent replication of a key-value store across a distributed system. The operations that the key-value store supports are get, put, delete, and update. To run a demo, do the following.

```bash
git clone git@github.com:ShivaanshK/Lamport-SMR.git
cd Lamport-SMR
```
```bash
# In the first shell
go run main.go -pid=0
# In the second shell
go run main.go -pid=1
# In the third shell
go run main.go -pid=2
# In the fourth shell
go run main.go -pid=3
```

***Note:*** You can add more processes to config.json to be able to run more processes. Processes need to be added as a full libp2p multiaddr.
