package surfstore

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

// Implement the logic for a client syncing with the server here.

// ClientSync synchronizes the local base directory with the cloud.
func ClientSync(client RPCClient) {
	// fmt.Println("in clientSync")
	basePath := client.BaseDir
	blockSize := client.BlockSize

	// Scan the base directory
	dirMetaMap, blocks, err := scanDirectory(basePath, blockSize) // map: base dir
	// PrintMetaMap(dirMetaMap)
	if err != nil {
		log.Fatalf("Failed to scan directory: %v", err)
	}

	// Load the local index file
	indexMetaMap, err := LoadMetaFromMetaFile(basePath) // map: local ind
	if err != nil {
		log.Fatalf("Failed to load index file: %v", err)
	}

	// Get the remote index from the server
	remoteMetaMap := make(map[string]*FileMetaData) // map: remote ind
	err = client.GetFileInfoMap(&remoteMetaMap)
	// PrintMetaMap(remoteMetaMap)
	if err != nil {
		log.Fatalf("Failed to get remote index: %v", err)
	}

	// Compare and sync

	// Step 1: pull from remote
	for fileName, _ := range remoteMetaMap {
		localMeta, localExists := indexMetaMap[fileName]
		remoteMeta := remoteMetaMap[fileName]
		_, dirExists := dirMetaMap[fileName]
		if !localExists {
			err = handleMissingLocalFile(client, fileName, remoteMeta, indexMetaMap, dirMetaMap)
			if err != nil {
				log.Fatalf("Failed to handle missing data: %v", err)
			}
		} else if localMeta.Version < remoteMeta.Version && localMeta.BlockHashList[0] != "0" {
			handleOutdatedLocalFile(client, fileName, remoteMetaMap[fileName], indexMetaMap, dirMetaMap, blocks)
		} else if localMeta.Version < remoteMeta.Version && localMeta.BlockHashList[0] == "0" {
			err = handleMissingLocalFile(client, fileName, remoteMeta, indexMetaMap, dirMetaMap)
			if err != nil {
				log.Fatalf("Failed to handle missing data: %v", err)
			}
		} else if !dirExists && remoteMeta.BlockHashList[0] != "0" {
			err = deleteFromRemote(client, fileName, indexMetaMap)
			if err != nil {
				log.Fatalf("Fail deleting: %v", err)
			}
		}
	}

	// load remote again
	remoteMetaMap = make(map[string]*FileMetaData) // map: remote ind
	err = client.GetFileInfoMap(&remoteMetaMap)
	if err != nil {
		log.Fatalf("Failed to get remote index: %v", err)
	}

	// Step 2: iter over base
	for fileName, fileMeta := range dirMetaMap {
		localMeta, localExists := indexMetaMap[fileName]
		remoteMeta, remoteExists := remoteMetaMap[fileName]

		if !localExists || !remoteExists {
			// New file in the local base directory
			err = uploadNewLocalFile(client, fileMeta, indexMetaMap, blocks)
			if err != nil {
				log.Printf("Failed to handle new local file %s: %v", fileName, err)
			}

		} else if !compareSlices(localMeta.BlockHashList, fileMeta.BlockHashList) &&
			localMeta.Version == remoteMeta.Version {
			// Local changes, upload to server
			err = handleModifiedLocalFile(client, fileMeta, indexMetaMap, blocks)
			if err != nil {
				log.Printf("Failed to handle modified local file %s: %v", fileName, err)
			}
		}
	}

	// Save the updated local index
	err = WriteMetaFile(indexMetaMap, basePath)
	if err != nil {
		log.Fatalf("Failed to write index file: %v", err)
	}
	// PrintMetaMap(indexMetaMap)
}

func readFileInBlocks(filePath string, blockSize int, blocks *map[string]*Block) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()
	var hashList []string
	for {
		buffer := make([]byte, blockSize)
		byteRead, err := file.Read(buffer)
		if byteRead == 0 {
			break
		}
		// Compute the hash of the block and store it in the hash list
		blockHash := GetBlockHashString(buffer[:byteRead])
		hashList = append(hashList, blockHash)
		// fmt.Println("readFileInBlock: copy", blockdata)
		block := Block{
			BlockData: buffer[:byteRead],
			BlockSize: int32(byteRead),
		}
		(*blocks)[blockHash] = &block
		if err != nil {
			if err == io.EOF {
				break
			}
		}
	}
	return hashList, nil
}

// scanDirectory recursively scans directories, reads files in blocks, hashes those blocks, and constructs a map.
func scanDirectory(dirPath string, blockSize int) (map[string]*FileMetaData, map[string]*Block, error) {
	entries, err := os.ReadDir(dirPath)
	fileMetaMap := make(map[string]*FileMetaData)
	blocks := make(map[string]*Block)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading directory %s: %w", dirPath, err)
	}

	for _, entry := range entries {
		filename := entry.Name()
		if filename == "index.db" || filename[0] == '.' {
			continue
		}

		fullPath := filepath.Join(dirPath, entry.Name())

		// Process file
		hashlist, err := readFileInBlocks(fullPath, blockSize, &blocks)
		// fmt.Println("scanDir hashlist len", len(hashlist))
		if err != nil {
			return nil, nil, err
		}

		fileMetaMap[filename] = &FileMetaData{
			Filename:      filename,
			Version:       -1,
			BlockHashList: hashlist,
		}

	}
	return fileMetaMap, blocks, nil
}

// handleNewLocalFile handles new files in the local base directory.
func uploadNewLocalFile(client RPCClient, fileMeta *FileMetaData,
	indexMetaMap map[string]*FileMetaData, blocks map[string]*Block) error {

	// Prepare the blockStore map
	blockStoreMap := make(map[string][]string)
	// fmt.Println("Utils uploadNewLocalFile hashlist len:", len(fileMeta.BlockHashList))
	err := client.GetBlockStoreMap(fileMeta.BlockHashList, &blockStoreMap)

	if err != nil {
		return err
	}
	// Upload the blocks
	for _, hash := range fileMeta.BlockHashList {
		var succ bool
		blockAddr, err := findAddr(&blockStoreMap, hash)
		// fmt.Println(blockAddr, ":", hash)
		if err != nil {
			return err
		}
		err = client.PutBlock(blocks[hash], blockAddr, &succ)
		if err != nil || !succ {
			// fmt.Println()
			return fmt.Errorf("failed to upload block to %s: %v", blockAddr, err)
		}
	}

	// Update the server with the new FileMetaData
	var latestVersion int32
	err = client.UpdateFile(fileMeta, &latestVersion)
	if err != nil {
		return err
	}

	// Update local index
	fileMeta.Version = latestVersion
	indexMetaMap[fileMeta.Filename] = &FileMetaData{
		Filename:      fileMeta.Filename,
		Version:       latestVersion,
		BlockHashList: fileMeta.BlockHashList,
	}
	return nil
}

// compareSlices checks if two slices of strings are equal.
func compareSlices(a, b []string) bool {
	if a == nil && b == nil {
		return true
	} else if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// handleModifiedLocalFile handles modified files in the local base directory.
func handleModifiedLocalFile(client RPCClient, fileMeta *FileMetaData,
	indexMetaMap map[string]*FileMetaData, blocks map[string]*Block) error {

	// Prepare the blockStore map
	blockStoreMap := make(map[string][]string)
	err := client.GetBlockStoreMap(fileMeta.BlockHashList, &blockStoreMap)
	if err != nil {
		return err
	}

	// Upload the blocks
	for _, hash := range fileMeta.BlockHashList {
		block := blocks[hash]
		var succ bool
		blockAddr, err := findAddr(&blockStoreMap, hash)
		if err != nil {
			return err
		}
		err = client.PutBlock(block, blockAddr, &succ)
		if err != nil || !succ {
			return fmt.Errorf("failed to upload block: %v", err)
		}
	}

	// Update the server with the new FileMetaData
	fileMeta.Version = indexMetaMap[fileMeta.Filename].Version + 1
	var latestVersion int32
	err = client.UpdateFile(fileMeta, &latestVersion)
	if err != nil {
		return err
	}

	// Update local index
	fileMeta.Version = latestVersion
	indexMetaMap[fileMeta.Filename] = fileMeta
	fmt.Println("SUCCESS COMMIT WITH V", latestVersion)
	return nil
}

// handleMissingLocalFile handles files that are in the remote index but not in the local base directory.
func handleMissingLocalFile(client RPCClient,
	fileName string, remoteMeta *FileMetaData, indexMetaMap map[string]*FileMetaData,
	fileMeta map[string]*FileMetaData) error {

	fmt.Println("Missing", fileName, "Downloading...")
	// get the block map
	blockStoreMap := make(map[string][]string)
	err := client.GetBlockStoreMap(remoteMeta.BlockHashList, &blockStoreMap)
	if err != nil {
		return fmt.Errorf("fail getting block store map %v", err)
	}
	// Download the blocks{
	fileData := make(map[string][]byte) // a map that store the hash,data pair
	filePath := filepath.Join(client.BaseDir, fileName)
	for blockAddr, hashlist := range blockStoreMap {
		for _, hash := range hashlist {
			block := &Block{}
			err := client.GetBlock(hash, blockAddr, block)
			if err != nil {
				return fmt.Errorf("failed to download block from %s: %v", blockAddr, err)
			}
			fileData[hash] = block.BlockData[:block.BlockSize]
		}
	}
	// Write hash, data map to filedata
	filebytes := make([]byte, 0)
	for _, hash := range remoteMeta.BlockHashList {
		filebytes = append(filebytes, fileData[hash]...)
	}

	// Write the reconstituted file to the base directory
	err = os.WriteFile(filePath, filebytes, 0644)
	if err != nil {
		return fmt.Errorf("failed to reconstitute file: %v", err)
	}

	// Update the local index with the remote metadata
	// indexMetaMap[fileName] = remoteMeta
	indexMetaMap[fileName] = remoteMeta
	fileMeta[fileName] = remoteMeta
	return nil
}

// handleOutdatedLocalFile handles files that are outdated locally and updates only the differing parts.
func handleOutdatedLocalFile(client RPCClient, fileName string,
	remoteMeta *FileMetaData, indexMetaMap map[string]*FileMetaData, fileMeta map[string]*FileMetaData,
	blocks map[string]*Block) error {

	filePath := filepath.Join(client.BaseDir, fileName)
	// Case1: deletion
	hashList := remoteMeta.BlockHashList
	if len(hashList) == 1 && hashList[0] == "0" {
		fmt.Println("Deleting", fileName)
		err := os.Remove(filePath)
		if err != nil {
			return fmt.Errorf("fail deleting %s: %v", fileName, err)
		}
		indexMetaMap[fileName] = remoteMeta
		fileMeta[fileName] = remoteMeta
		return nil
	}
	// Case2: modified
	fmt.Println("OutDated", fileName, "Downloading...")
	// get the block map
	blockStoreMap := make(map[string][]string)
	err := client.GetBlockStoreMap(remoteMeta.BlockHashList, &blockStoreMap)
	if err != nil {
		return fmt.Errorf("fail getting block store map %v", err)
	}
	// Read the existing file data
	existingFileData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read existing file: %v", err)
	}
	updatedFileData := make([]byte, 0)
	localHashList := indexMetaMap[fileName].BlockHashList
	// Download and update only the differing blocks
	startIndex := 0
	endIndex := 0
	for i, hash := range remoteMeta.BlockHashList {
		if i >= len(localHashList) || hash != localHashList[i] {
			blockAddr, err := findAddr(&blockStoreMap, hash)
			if err != nil {
				return err
			}
			// get the block map
			block := &Block{}
			err = client.GetBlock(hash, blockAddr, block)
			if err != nil {
				return fmt.Errorf("failed to download block: %v", err)
			}
			endIndex = startIndex + int(block.BlockSize)
			updatedFileData = append(updatedFileData, block.BlockData...)
			startIndex = endIndex
		} else {
			// startIndex += int(blocks[hash].BlockSize)
			endIndex = startIndex + int(blocks[hash].BlockSize)
			updatedFileData = append(updatedFileData, existingFileData[startIndex:endIndex]...)
			startIndex = endIndex
		}
	}
	// Write the updated file data back to disk
	err = os.WriteFile(filePath, updatedFileData[:endIndex], 0644)
	if err != nil {
		return fmt.Errorf("failed to write updated file: %v", err)
	}

	// Update the local index with the remote metadata
	indexMetaMap[fileName] = remoteMeta
	fileMeta[fileName] = remoteMeta
	return nil
}

// handleModifiedLocalFile handles modified files in the local base directory.
func deleteFromRemote(client RPCClient, fileName string,
	indexMetaMap map[string]*FileMetaData) error {

	// Update the server with the new FileMetaData
	version := indexMetaMap[fileName].Version + 1
	meta := FileMetaData{
		Filename:      fileName,
		Version:       version,
		BlockHashList: []string{"0"},
	}
	var latestVersion int32
	err := client.UpdateFile(&meta, &latestVersion)
	if err != nil {
		return err
	}

	// Update local index
	indexMetaMap[fileName] = &meta
	fmt.Println("SUCCESS DELETING", fileName, latestVersion)
	return nil
}

func findAddr(bmap *map[string][]string, hash string) (string, error) {
	for addr, hashList := range *bmap {
		for _, has := range hashList {
			if has == hash {
				return addr, nil
			}
		}
	}
	return "", fmt.Errorf("Fail to find hash")
}
