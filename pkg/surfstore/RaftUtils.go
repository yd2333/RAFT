package surfstore

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here
	conns := make([]*grpc.ClientConn, 0)
	for _, addr := range config.RaftAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
	}

	serverStatusMutex := sync.RWMutex{}
	raftStateMutex := sync.RWMutex{}

	server := RaftSurfstore{
		serverStatus:      ServerStatus_FOLLOWER,
		serverStatusMutex: &serverStatusMutex,
		term:              0,
		metaStore:         NewMetaStore(config.BlockAddrs),
		log:               make([]*UpdateOperation, 0),

		id:          id,
		commitIndex: -1,

		unreachableFrom: make(map[int64]bool),
		grpcServer:      grpc.NewServer(),
		rpcConns:        conns,

		raftStateMutex: &raftStateMutex,

		//New Additions
		peers:           config.RaftAddrs,
		pendingRequests: make([]*chan PendingRequest, 0),
		lastApplied:     -1,
		nextId:          make([]int64, len(config.RaftAddrs)),
		pendingCommit:   make([]*chan FileMetaData, 0),
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	RegisterRaftSurfstoreServer(server.grpcServer, server)

	log.Println("Successfully started the RAFT server with id:", server.id)
	l, e := net.Listen("tcp", server.peers[server.id])

	if e != nil {
		return e
	}

	return server.grpcServer.Serve(l)
}

func (s *RaftSurfstore) checkStatus() error {
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()

	if serverStatus == ServerStatus_CRASHED {
		return ErrServerCrashed
	}

	if serverStatus != ServerStatus_LEADER {
		return ErrNotLeader
	}

	return nil
}

func (s *RaftSurfstore) sendPersistentHeartbeats(ctx context.Context, reqId int64) {
	// fmt.Println("sendPersistentHeartbeats: nextId of", s.id, s.nextId)
	numServers := len(s.peers)
	peerResponses := make(chan bool, numServers-1)

	for idx := range s.peers {

		idx := int64(idx)

		if idx == s.id {
			peerResponses <- true
			continue
		}

		entriesToSend := s.log[s.nextId[idx]:]
		go s.sendToFollower(ctx, idx, entriesToSend, peerResponses)
	}

	totalResponses := 1
	numAliveServers := 1
	for totalResponses < numServers {
		fmt.Println(totalResponses, numServers)
		response := <-peerResponses
		totalResponses += 1
		if response {
			numAliveServers += 1
		}
	}

	if numAliveServers > numServers/2 {
		s.raftStateMutex.RLock()
		requestLen := int64(len(s.pendingRequests))
		s.raftStateMutex.RUnlock()

		if reqId >= 0 && reqId < requestLen {
			// fmt.Println("sendPersistent reqId:", reqId, "requestLen", requestLen)
			s.raftStateMutex.Lock()
			*s.pendingRequests[reqId] <- PendingRequest{success: true, err: nil}
			s.pendingRequests = append(s.pendingRequests[:reqId], s.pendingRequests[reqId+1:]...)
			s.raftStateMutex.Unlock()
		}
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context,
	peerId int64, entries []*UpdateOperation, peerResponses chan<- bool) {
	// This client is actually follower server
	// fmt.Println("sendToFollower to", peerId)
	// time.Sleep(10 * time.Millisecond)
	// fmt.Println("sendToFollower sleep done", peerId)
	client := NewRaftSurfstoreClient(s.rpcConns[peerId])

	// while until success
	success := false
	prevLogIndex := s.nextId[peerId] - 1
	// decrement := true
	for true {
		s.raftStateMutex.RLock()
		appendEntriesInput := AppendEntryInput{
			Term:         s.term,
			LeaderId:     s.id,
			PrevLogTerm:  s.term,
			PrevLogIndex: prevLogIndex, // -1, 1, ...
			Entries:      entries,
			LeaderCommit: s.commitIndex,
		}
		// PrintAppendEntriesInput(&appendEntriesInput)
		s.raftStateMutex.RUnlock()

		// fmt.Println("sendToFollower:", peerId, "calling appendEntries")
		time.Sleep(750 * time.Millisecond)
		reply, err := client.AppendEntries(ctx, &appendEntriesInput)
		// time.Sleep(250 * time.Millisecond)

		if err != nil {
			peerResponses <- false
			return
		}
		// PrintAppendEntriesOutput(reply)
		success = reply.Success
		// case 1: success updates
		s.raftStateMutex.RLock()
		if success {
			s.lastApplied = reply.MatchedIndex
			s.nextId[peerId] = reply.MatchedIndex + 1
			// fmt.Println("sendToFollower,", peerId)
			peerResponses <- true
			s.raftStateMutex.RUnlock()
			fmt.Println("sendToFollower:Appendentries to", peerId)
			return
		}
		fmt.Println("sendToFollower:Appendentries to fail", peerId)
		peerResponses <- false
		s.raftStateMutex.RUnlock()
		return
		// case 2: if term conflict, then step down, break and return fail
		// if reply.Term != appendEntriesInput.Term {
		// 	peerResponses <- false
		// 	return
		// }
		// prevLogIndex -= 1
		// return
	}
}

func PrintAppendEntriesInput(appendEntriesInput *AppendEntryInput) {
	fmt.Println("AppendEntryInput:")
	fmt.Println("	Term:", appendEntriesInput.Term)
	fmt.Println("	LeaderId:", appendEntriesInput.LeaderId)
	fmt.Println("	PrevLogTerm:", appendEntriesInput.PrevLogTerm)
	fmt.Println("	PrevLogIndex:", appendEntriesInput.PrevLogIndex)
	fmt.Println("	Entries:", appendEntriesInput.Entries)
	fmt.Println("	LeaderCommit:", appendEntriesInput.LeaderCommit)
}

func PrintAppendEntriesOutput(o *AppendEntryOutput) {
	fmt.Println("AppendEntryOutput:")
	fmt.Println("	ServerId:", o.ServerId)
	fmt.Println("	Term:", o.Term)
	fmt.Println("	Success:", o.Success)
	fmt.Println("	matchInd:", o.MatchedIndex)
}

// func (s *RaftSurfstore) ApplyToStateMachine(ctx context.Context, applyId int64) {
// 	s.raftStateMutex.RLock()
// 	fileMeta := <-(*s.pendingCommit[applyId])
// 	s.raftStateMutex.RUnlock()
// 	s.metaStore.UpdateFile(ctx, &fileMeta)
// }
