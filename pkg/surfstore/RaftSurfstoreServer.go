package surfstore

import (
	context "context"
	"fmt"
	"log"
	"sync"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	serverStatus      ServerStatus
	serverStatusMutex *sync.RWMutex
	term              int64
	log               []*UpdateOperation
	id                int64
	metaStore         *MetaStore
	commitIndex       int64

	raftStateMutex *sync.RWMutex

	rpcConns   []*grpc.ClientConn
	grpcServer *grpc.Server

	//New Additions
	peers           []string
	pendingRequests []*chan PendingRequest // channel of pending request from client
	lastApplied     int64
	nextId          []int64 // entry to send
	pendingCommit   []*chan FileMetaData
	/*--------------- Chaos Monkey --------------*/
	unreachableFrom map[int64]bool
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Ensure that the majority of servers are up
	s.sendPersistentHeartbeats(ctx, int64(0))
	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// Ensure that the majority of servers are up
	s.sendPersistentHeartbeats(ctx, int64(0))
	return s.metaStore.GetBlockStoreMap(ctx, hashes)

}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// Ensure that the majority of servers are up
	s.sendPersistentHeartbeats(ctx, int64(0))
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)

}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// Ensure that the request gets replicated on majority of the servers.
	// Commit the entries and then apply to the state machine

	// Check current server is leader
	if err := s.checkStatus(); err != nil {
		return nil, err
	}

	// Commit the preceding
	for s.lastApplied < int64(len(s.log)-1) {
		filemeta := s.log[s.lastApplied+1].FileMetaData
		s.metaStore.UpdateFile(ctx, filemeta)
		s.lastApplied += 1
		s.commitIndex = s.lastApplied
	}
	// add client request to log, pending request
	pendingReq := make(chan PendingRequest)
	s.raftStateMutex.Lock()
	entry := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, &entry)
	s.pendingRequests = append(s.pendingRequests, &pendingReq)
	fmt.Println("updateFile: server ", s.id, "log", s.log)

	//TODO: Think whether it should be last or first request
	// last
	reqId := len(s.pendingRequests) - 1
	s.raftStateMutex.Unlock()

	// Majority success
	go s.sendPersistentHeartbeats(ctx, int64(reqId))
	response := <-pendingReq
	if response.err != nil {
		return nil, response.err
	}

	//TODO:
	// Ensure that leader commits first and then applies to the state machine
	s.raftStateMutex.Lock()
	s.commitIndex += 1
	s.raftStateMutex.Unlock()
	s.lastApplied += 1
	if filemeta == nil {
		return nil, nil
	}
	fmt.Println("Update Done")
	return s.metaStore.UpdateFile(ctx, filemeta)
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex or whose term
// doesn't match prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// Preparation
	s.raftStateMutex.RLock()
	peerTerm := s.term
	peerId := s.id
	s.raftStateMutex.RUnlock()
	// if peerId != 10 {
	// 	fmt.Println("Appendentries: id", peerId)
	// 	PrintAppendEntriesInput(input)
	// }
	// prepare appendEntryOutput according to different cases
	// Case 1: Stepping down
	output := AppendEntryOutput{}
	// step down: outdated
	if peerTerm < input.Term {
		s.serverStatusMutex.Lock()
		s.serverStatus = ServerStatus_FOLLOWER
		s.serverStatusMutex.Unlock()
		// update term
		s.raftStateMutex.Lock()
		s.term = input.Term
		s.raftStateMutex.Unlock()
		peerTerm = input.Term
		// fmt.Println("Appendentries: id", peerId, " stepping down")
		return &AppendEntryOutput{
			Term:         peerTerm,
			ServerId:     peerId,
			Success:      true,
			MatchedIndex: -1,
		}, nil
	}
	// Case 2: Reply false if log doesn’t contain an entry at prevLogIndex
	// or whose term matches prevLogTerm
	if input.PrevLogIndex >= 0 &&
		(input.PrevLogIndex >= int64(len(s.log)) ||
			s.log[input.PrevLogIndex].Term != input.PrevLogTerm) {
		output = AppendEntryOutput{
			Term:         input.Term,
			ServerId:     s.id,
			Success:      false,
			MatchedIndex: -1,
		}
		return &output, nil
	}

	// Case 3: If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	for i, entry := range input.Entries {
		logIndex := input.PrevLogIndex + 1 + int64(i)
		// fmt.Println("appendentries case 3: logIndex", logIndex, "input.PrevLogIndex", input.PrevLogIndex)
		if logIndex < int64(len(s.log)) && s.log[logIndex].Term != entry.Term {
			s.log = s.log[:logIndex] // equivalent to deleting all the following
			break
		}
	}

	// Step 4: Append any new entries not already in the log
	// fmt.Println("appendEntries: input.Entries", input.Entries, "logIndex", input.PrevLogIndex+1, "len(s.log)", len(s.log))
	s.serverStatusMutex.Lock()
	for i, entry := range input.Entries {
		logIndex := input.PrevLogIndex + 1 + int64(i)
		if logIndex >= int64(len(s.log)) {
			s.log = append(s.log, entry)
		}
	}
	s.commitIndex = int64(len(s.log))
	s.serverStatusMutex.Unlock()

	//Case 4: if input.LeaderCommit > s.commitIndex {
	if s.commitIndex > input.LeaderCommit {
		s.commitIndex = input.LeaderCommit
	}
	// s.commitIndex = input.LeaderCommit

	// Apply to local state machine
	// if peerId == 1 {
	fmt.Println("appendEntries log:", s.log, "s.lastApplied", s.lastApplied, "input.LeaderCommit", input.LeaderCommit)
	// }
	s.raftStateMutex.Lock()
	for s.lastApplied < input.LeaderCommit {
		fmt.Println("appendEntries, s.lastApplied+1", s.lastApplied+1, peerId)
		entry := s.log[s.lastApplied+1]
		// fmt.Println("appendEntries ")
		if entry.FileMetaData == nil {
			// fmt.Println("Nil File Meta")
			s.lastApplied += 1
			continue
		}
		// fmt.Println("appendEntries: server", s.id, "updating ", entry.FileMetaData.Filename, entry.FileMetaData.Version)
		fmt.Println("AppendEntries: non nil meta", peerId, entry.FileMetaData)
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		// _, err := s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		// if err != nil {
		// 	s.raftStateMutex.Unlock()
		// 	return nil, err
		// }
		s.lastApplied += 1
	}

	s.term = input.Term
	output.Term = s.term
	output.ServerId = s.id
	output.Success = true
	output.MatchedIndex = s.commitIndex
	s.raftStateMutex.Unlock()
	// fmt.Println("AppendEntries leaving: ")
	// PrintAppendEntriesOutput(&output)
	// fmt.Println("AppendEntries leaving: ", s.id, "has log ", s.log) // log yes
	return &output, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()

	if serverStatus == ServerStatus_CRASHED {
		return &Success{Flag: false}, ErrServerCrashed
	}

	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_LEADER
	log.Printf("Server %d has been set as a leader", s.id)
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.term += 1
	s.raftStateMutex.Unlock()

	//TODO: update the state

	// add client request to log, pending request
	// pendingReq := make(chan PendingRequest)
	// s.raftStateMutex.Lock()
	// entry := UpdateOperation{
	// 	Term:         s.term,
	// 	FileMetaData: nil,
	// }
	// s.log = append(s.log, &entry)
	// s.pendingRequests = append(s.pendingRequests, &pendingReq)

	//TODO: Think whether it should be last or first request
	// last
	// reqId := len(s.pendingRequests) - 1
	// s.raftStateMutex.Unlock()

	_, err := s.UpdateFile(ctx, nil)
	if err != nil {
		return &Success{Flag: false}, err
	}
	// s.sendPersistentHeartbeats(ctx, int64(reqId))

	// response := <-pendingReq
	// if response.err != nil {
	// 	return nil, response.err
	// }

	//TODO:
	// Ensure that leader commits first and then applies to the state machine
	// s.raftStateMutex.Lock()
	// s.commitIndex += 1
	// s.raftStateMutex.Unlock()
	//******
	// fmt.Println("setLeader:server ", s.id, "log", s.log, "pendingreq len:", len(s.pendingRequests))
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if err := s.checkStatus(); err != nil {
		return nil, err
	}

	// s.raftStateMutex.RLock()
	// reqId := len(s.pendingRequests) - 1
	// s.raftStateMutex.RUnlock()
	// fmt.Println("SendHeartBeat: len of pendingRequests", len(s.pendingRequests))
	s.sendPersistentHeartbeats(ctx, int64(0))
	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) MakeServerUnreachableFrom(ctx context.Context, servers *UnreachableFromServers) (*Success, error) {
	s.raftStateMutex.Lock()
	for _, serverId := range servers.ServerIds {
		s.unreachableFrom[serverId] = true
	}
	log.Printf("Server %d is unreachable from", s.unreachableFrom)
	s.raftStateMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_CRASHED
	log.Printf("Server %d is crashed", s.id)
	s.serverStatusMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_FOLLOWER
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.unreachableFrom = make(map[int64]bool)
	s.raftStateMutex.Unlock()

	log.Printf("Server %d is restored to follower and reachable from all servers", s.id)

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.serverStatusMutex.RLock()
	s.raftStateMutex.RLock()
	state := &RaftInternalState{
		Status:      s.serverStatus,
		Term:        s.term,
		CommitIndex: s.commitIndex,
		Log:         s.log,
		MetaMap:     fileInfoMap,
	}
	s.raftStateMutex.RUnlock()
	s.serverStatusMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
