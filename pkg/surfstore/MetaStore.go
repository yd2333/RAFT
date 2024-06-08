package surfstore

import (
	context "context"
	"fmt"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
	sync.Mutex
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	fileInfoMap := &FileInfoMap{
		FileInfoMap: make(map[string]*FileMetaData),
	}
	for fileName, metaData := range m.FileMetaMap {
		clonedMetaData := &FileMetaData{
			Filename:      metaData.Filename,
			Version:       metaData.Version,
			BlockHashList: append([]string(nil), metaData.BlockHashList...),
		}
		fileInfoMap.FileInfoMap[fileName] = clonedMetaData
	}
	return fileInfoMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// panic("todo")
	m.Lock()
	defer m.Unlock()

	existingMetaData, exists := m.FileMetaMap[fileMetaData.Filename]
	nowVer := int32(0)
	if exists {
		// Check for version conflicts
		expected_version := existingMetaData.Version + 1
		if fileMetaData.Version != expected_version {
			return nil, fmt.Errorf("version conflict: new version expected to be %d, now have %d", expected_version, fileMetaData.Version)
		}
		// Update the existing metadata
		existingMetaData.Version += 1
		nowVer = existingMetaData.Version
		existingMetaData.BlockHashList = fileMetaData.BlockHashList
	} else {
		// Add new metadata
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		nowVer = 1
		m.FileMetaMap[fileMetaData.Filename].Version = nowVer
		// hashList := fileMetaData.BlockHashList
		// if len(hashList) == 0 && hashList[0]
	}
	// PrintMetaMap(m.FileMetaMap)
	// fmt.Println("META: SUCCESS UPDATING", fileMetaData.Filename, "v", nowVer)
	return &Version{Version: int32(nowVer)}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	mapping := make(map[string]*BlockHashes)
	// fmt.Println("created mapping")
	for _, hash := range blockHashesIn.Hashes {
		dest := m.ConsistentHashRing.GetResponsibleServer(hash)
		// fmt.Println("responsible server is",dest)
		// If it does not exist, initialize it
		if _, exists := mapping[dest]; !exists {
			mapping[dest] = &BlockHashes{Hashes: []string{}}
		}
		mapping[dest].Hashes = append(mapping[dest].Hashes, hash)
	}
	return &BlockStoreMap{BlockStoreMap: mapping}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
