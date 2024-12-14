package SurfTest

import (
	"cse224/proj5/pkg/surfstore"
	"fmt"
	"log"
	"testing"
	"time"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(1) {
			t.Fatalf("Server %d should be in term %d but now %d", idx, 1, state.Term)
		}
		if idx == leaderIdx {
			// server should be the leader
			if state.Status != surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.Status == surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}
	fmt.Println("TEST: CHANGE LEADER TO 2!!!!!")

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(2) {
			t.Fatalf("Server should be in term %d", 2)
		}
		if idx == leaderIdx {
			// server should be the leader
			if state.Status != surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.Status == surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}
}

func TestRaftFollowersGetUpdates(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	fmt.Println("____set leader done_______________________________________________")
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	fmt.Println("____SendHeartbeat done_______________________________________________")

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
	fmt.Println("____UpdateFile done_______________________________________________")

	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	fmt.Println("____SendHeartbeat_______________________________________________")

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta1.Filename] = filemeta1

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: nil,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})
	var leader bool
	term := int64(1)

	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}
		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}

func TestRaftLogsConsistentLeaderCrashesBeforeHeartbeat(t *testing.T) {
	/*raft_test.go:525:
	leader1 gets a request while a minority of the cluster is down.
	leader1 crashes before sending a heartbeat.
	the other crashed nodes are restored.
	leader2 gets a request.
	leader1 is restored.
	*/
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	// 1st crash
	crashedIdx := 1
	test.Clients[crashedIdx].Crash(test.Context, &emptypb.Empty{})
	// TEST
	leader1 := 0 // leader 1
	test.Clients[leader1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leader1].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile2",
		Version:       1,
		BlockHashList: nil,
	}

	// update
	test.Clients[leader1].UpdateFile(test.Context, filemeta1)
	// leader crash before sending heart beat  & restore 1st crashed
	test.Clients[leader1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[crashedIdx].Restore(test.Context, &emptypb.Empty{})

	leader2 := 2
	test.Clients[leader2].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leader2].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[leader2].UpdateFile(test.Context, filemeta2)
	test.Clients[leader1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[leader2].SendHeartbeat(test.Context, &emptypb.Empty{})

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta1.Filename] = filemeta1
	goldenMeta[filemeta2.Filename] = filemeta2

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: nil,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: nil,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: filemeta2,
	})
	var leader bool
	term := int64(2)

	for idx, server := range test.Clients {
		if idx == leader2 {
			leader = bool(true)
		} else {
			leader = bool(false)
		}
		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}

func TestRaftSetLeader5Nodes(t *testing.T) {
	//Setup
	cfgPath := "./config_files/5nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(1) {
			t.Fatalf("Server %d should be in term %d but now %d", idx, 1, state.Term)
		}
		if idx == leaderIdx {
			// server should be the leader
			if state.Status != surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.Status == surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}
	fmt.Println("TEST: CHANGE LEADER TO 2!!!!!")

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(2) {
			t.Fatalf("Server should be in term %d", 2)
		}
		if idx == leaderIdx {
			// server should be the leader
			if state.Status != surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.Status == surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}
}

func TestRaftFollowersGetUpdates5nodes(t *testing.T) {
	//Setup
	cfgPath := "./config_files/5nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	fmt.Println("____set leader done_______________________________________________")
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	fmt.Println("____SendHeartbeat done_______________________________________________")

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
	fmt.Println("____UpdateFile done_______________________________________________")

	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	fmt.Println("____SendHeartbeat_______________________________________________")

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta1.Filename] = filemeta1

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: nil,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})
	var leader bool
	term := int64(1)

	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}
		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}

func TestRaftNetworkPartitionLogsOverwritten(t *testing.T) {
	/*
		raft_test.go:920:
		leader1 gets a request while the majority of the cluster is unreachable.
		As a result of a (one way) network partition, leader1 ends up with the minority.
		leader2 from the majority is elected

	*/
	//Setup
	cfgPath := "./config_files/5nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leader1 := 0
	leader2 := 2
	test.Clients[leader1].SetLeader(test.Context, &emptypb.Empty{})
	fmt.Println("____set leader done_______________________________________________")
	test.Clients[leader1].SendHeartbeat(test.Context, &emptypb.Empty{})
	fmt.Println("____SendHeartbeat done_______________________________________________")

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	unreachables := []int64{2, 3, 4}
	test.Clients[leader1].MakeServerUnreachableFrom(test.Context, &surfstore.UnreachableFromServers{ServerIds: unreachables})
	test.Clients[leader1].UpdateFile(test.Context, filemeta1)

	test.Clients[leader1].SendHeartbeat(test.Context, &emptypb.Empty{})
	fmt.Println("____SendHeartbeat done_______________________________________________")

	test.Clients[leader2].SetLeader(test.Context, &emptypb.Empty{})

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	// goldenMeta[filemeta1.Filename] = filemeta1

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: nil,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: nil,
	})
	var leader bool
	term := int64(1)

	for idx, server := range test.Clients {
		if idx == leader2 {
			leader = bool(true)
		} else {
			leader = bool(false)
		}
		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}

func TestRaftNetworkPartitionWithConcurrentRequests(t *testing.T) {
	t.Log("leader1 gets 1 request while the majority of the cluster is unreachable. As a result of a (one way) network partition, leader1 ends up with the minority. leader2 from the majority is elected")
	//  // A B C D E
	cfgPath := "./config_files/5nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile2",
		Version:       1,
		BlockHashList: nil,
	}

	A := 0
	//B := 1
	C := 2
	//D := 3
	E := 4

	// A is leader
	leaderIdx := A
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Partition happens
	// C D E are unreachable
	unreachableFromServers := &surfstore.UnreachableFromServers{
		ServerIds: []int64{0, 1},
	}
	for i := C; i <= E; i++ {
		test.Clients[i].MakeServerUnreachableFrom(test.Context, unreachableFromServers)
	}

	blockChan := make(chan bool)

	// A gets an entry and pushes to A and B
	go func() {
		// This should block though and fail to commit when getting the RPC response from the new leader
		log.Println("****           A gets an entry and pushes to A and B")
		_, _ = test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
		blockChan <- false
	}()

	go func() {
		<-time.NewTimer(5 * time.Second).C
		blockChan <- true
	}()

	if !(<-blockChan) {
		t.Fatalf("Request did not block")
	}

	// C becomes leader
	leaderIdx = C
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	// C D E are restored
	for i := C; i <= E; i++ {
		test.Clients[i].MakeServerUnreachableFrom(test.Context, &surfstore.UnreachableFromServers{ServerIds: []int64{}})
	}

	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	time.Sleep(time.Second)

	// Every node should have an empty log
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: nil,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: nil,
	})
	// Leaders should not commit entries that were created in previous terms.
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	term := int64(2)

	for idx, server := range test.Clients {

		_, err := CheckInternalState(nil, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}

	go func() {
		test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
		test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	}()
	go func() {
		test.Clients[leaderIdx].UpdateFile(test.Context, filemeta2)
		test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	}()

	//Enough time for things to settle
	time.Sleep(2 * time.Second)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	goldenMeta[filemeta1.Filename] = filemeta1
	goldenMeta[filemeta2.Filename] = filemeta2

	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: filemeta1,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: filemeta2,
	})

	for idx, server := range test.Clients {

		state, err := server.GetInternalState(test.Context, &emptypb.Empty{})
		if err != nil {
			t.Fatalf("could not get internal state: %s", err.Error())
		}
		if state == nil {
			t.Fatalf("state is nil")
		}

		if len(state.Log) != 4 {
			t.Fatalf("Should have 4 logs")
		}

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}
