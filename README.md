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
...
```
