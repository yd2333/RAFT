package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// panic("todo")
	hashes := []string{}
	serverMap := c.ServerMap
	for h := range c.ServerMap {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	// find the first server with larger hash value than blockHash
	responsibleServer := ""
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			responsibleServer = serverMap[hashes[i]]
			break
		}
	}
	if responsibleServer == "" {
		responsibleServer = serverMap[hashes[0]]
	}
	return responsibleServer

}

func Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	// hash servers on a hash ring
	consistentHashRing := make(map[string]string) // hash: serverName
	for _, serverAddr := range serverAddrs {
		serverName := fmt.Sprintf("blockstore%s", serverAddr)
		serverHash := Hash(serverName)
		consistentHashRing[serverHash] = serverAddr
	}
	return &ConsistentHashRing{ServerMap: consistentHashRing}
}
