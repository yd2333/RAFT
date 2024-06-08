package surfstore

import (
	context "context"
	"fmt"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	block, exists := bs.BlockMap[blockHash.Hash]
	if !exists {
		return nil, fmt.Errorf("not exist: looking for %v", blockHash.Hash)
	}
	return block, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	bs.Lock()
	defer bs.Unlock()
	// Compute the hash of the block data
	hashString := GetBlockHashString(block.BlockData)

	// Store the block in the BlockMap
	bs.BlockMap[hashString] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// hashes that are not stored in the key-value store
func (bs *BlockStore) MissingBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	bs.Lock()
	defer bs.Unlock()

	var missingHashes []string
	for _, hash := range blockHashesIn.Hashes {
		if _, exists := bs.BlockMap[hash]; !exists {
			missingHashes = append(missingHashes, hash)
		}
	}

	return &BlockHashes{Hashes: missingHashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	// panic("todo")

	if bs == nil {
		return nil, fmt.Errorf("BlockStore is nil")
	}
	if bs.BlockMap == nil {
		return nil, nil
	}
	hashes := make([]string, 0, len(bs.BlockMap))
	for key := range bs.BlockMap {
		hashes = append(hashes, key)
	}
	return &BlockHashes{Hashes: hashes}, nil

}
