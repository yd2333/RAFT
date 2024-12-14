# Fault-tolerant SurfStore

## Overview

Fault-tolerant SurfStore is a distributed metadata storage system designed to ensure consistency and availability even in the presence of server failures. It is built using the RAFT consensus protocol to provide a fault-tolerant MetaStore service. This system supports multiple replicated servers, ensuring that clients can reliably interact with the metadata service as long as a majority of the servers are operational.

## Key Features

1. **Metadata Replication**: Implements RAFT-based log replication to ensure all metadata updates are consistently propagated across the cluster.
2. **Leader-based Architecture**: Uses RAFT to elect a leader responsible for coordinating metadata updates, ensuring data integrity and consistency.
3. **Fault Tolerance**: Handles server crashes and network partitions gracefully. Clients retry interactions to locate an operational leader.
4. **Immutability**: Integrates seamlessly with a block store where data blocks are immutable, simplifying the replication process for stored data.
5. **Simulated Failures**: Provides mechanisms to simulate server crashes and unreachability for testing and robustness verification.

## Architecture

The system consists of the following components:

- **RaftSurfstoreServer**: A gRPC-based metadata server implementing the RAFT consensus protocol for fault-tolerant metadata storage.
- **BlockStore**: A server responsible for storing immutable data blocks, supporting operations like `putblock` and `getblock`.
- **Client**: A client application that interacts with the metadata and block storage servers to perform file operations.

### RAFT Protocol Implementation
The RAFT protocol is implemented with the following features:
- **Log Replication**: The leader appends updates to its local log and replicates them across followers.
- **Majority Quorum**: Metadata updates are committed once acknowledged by a majority of the servers.
- **Leader Completeness**: The leader ensures that committed entries are preserved and applied in order.
- **Simulated Failures**: Supports operations like crashing servers, restoring them, and simulating network partitions to test fault tolerance.

## Installation and Setup

### Prerequisites
- Go programming language (version 1.16 or higher)
- gRPC libraries and dependencies

### Getting Started
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd Fault-tolerant-SurfStore
```

2. Build the project:
   ```bash
   go build
   ```

3. Run the server:
   ```bash
   make run-blockstore
   ```

4. Run a RaftSurfstoreServer instance

   ```bash
   make IDX=0 run-raft
   ```

5. Run the client:
   ```bash
   go run cmd/SurfstoreClientExec/main.go -f configfile.txt baseDir blockSize
   ```

### Configuration
The servers and clients use a configuration file in JSON format to specify the addresses of MetaStore and BlockStore servers. An example configuration file (example_config.txt) is included in the repository.

### Testing
```bash
{
  "MetaStoreAddrs": ["localhost:8081", "localhost:8082", "localhost:8083"],
  "BlockStoreAddrs": ["localhost:8080"]
}
```

### Design Notes
#### Log Replication
Updates are logged by the leader and replicated to followers before being applied.
A majority quorum ensures consistency across the cluster.
Fault Handling
Crashed servers return appropriate errors and do not update their internal state.
Clients retry with alternative servers until a leader is found or a majority is restored.
#### Leader Management
Leaders are programmatically set via the SetLeader() API for simplicity.
Heartbeats are triggered using the SendHeartbeat() API to maintain leadership.
#### Limitations
Dynamic membership changes (e.g., adding new servers) and log compaction are not implemented.
Persistent storage for the log is not included; all data is stored in memory.
#### Resources
RAFT Paper: Key sections: 1, 2, 4, 5, 8, and 11.
RAFT Visualization Tool: Interactive visualization of the RAFT protocol.