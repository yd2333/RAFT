package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	s := hex.EncodeToString(blockHash)
	// fmt.Println("Generating Hash: ", s)
	return s
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = ``

// to do
// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	_, err = db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	defer db.Close()

	// Create the table for storing metadata
	createTable := `
	CREATE TABLE IF NOT EXISTS indexes (
		fileName TEXT,
		version INTEGER,
		hashIndex INTEGER,
		hashValue TEXT,
		PRIMARY KEY (fileName, hashIndex)
	);`
	_, err = db.Exec(createTable)
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}

	// Prepare the insert statement for inserting metadata
	insertStmt, err := db.Prepare("INSERT INTO indexes (fileName, version, hashIndex, hashValue) VALUES (?, ?, ?, ?)")
	if err != nil {
		log.Fatalf("Error preparing insert statement: %v", err)
	}
	defer insertStmt.Close()

	// Iterate over the file metadata map and insert each hash
	for _, metaData := range fileMetas {
		for hashIndex, hashValue := range metaData.BlockHashList {
			// fmt.Println(hashIndex, hashValue)
			_, err := insertStmt.Exec(metaData.Filename, metaData.Version, hashIndex, hashValue)
			if err != nil {
				log.Fatalf("Error inserting metadata for file %s: %v", metaData.Filename, err)
			}
		}
	}

	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = ``

const getTuplesByFileName string = ``

// To Do
// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	defer db.Close()

	// Prepare and execute the query to fetch file metadata
	query := `SELECT fileName, version, hashIndex, hashValue FROM indexes ORDER BY fileName, hashIndex`
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal("Error querying the database:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fileName string
		var version int32
		var hashIndex int
		var hashValue string

		err := rows.Scan(&fileName, &version, &hashIndex, &hashValue)
		if err != nil {
			log.Fatal("Error reading row:", err)
		}

		// Check if we already have metadata for this file
		if metaData, exists := fileMetaMap[fileName]; exists {
			// File already in map, update the BlockHashList directly
			metaData.BlockHashList = append(metaData.BlockHashList, hashValue)
		} else {
			// New file, create new metadata entry
			// hashlist = make([]string)
			var hashList []string
			hashList = append(hashList, hashValue)
			fileMetaMap[fileName] = &FileMetaData{
				Filename:      fileName,
				Version:       version,
				BlockHashList: hashList,
			}

		}
	}

	// Check for any error occurred during iteration
	if err = rows.Err(); err != nil {
		log.Fatal("Error during rows iteration:", err)
	}

	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}

func PrintBlockMap(blocks map[string]*Block) {

	fmt.Println("--------BEGIN PRINT Block--------")

	for hash, block := range blocks {
		fmt.Println("\t", hash, block.BlockSize)
		fmt.Println("\t\t", block.BlockData)
	}

	fmt.Println("---------END PRINT Block--------")

}

func Hash_to_string(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

// func consistentHashing(servers []string, blockHashesIn *BlockHashes,
// 	m *MetaStore) *BlockStoreMap {

// 	blockStoreMap := make(map[string]*BlockHashes)
// 	serverMap := m.ConsistentHashRing.ServerMap
// 	data := blockHashesIn.Hashes
// 	// find where each block belongs to
// 	// 1. sort hash values (key in hash ring)
// 	// hashes := []string{}
// 	// for h := range serverMap {
// 	// 	hashes = append(hashes, h)
// 	// }
// 	// sort.Strings(hashes)
// 	// // 2. find the first server with larger hash value than blockHash
// 	// for _, dat := range data {
// 	// 	blockHash := Hash_to_string(dat)
// 	// 	responsibleServer := ""
// 	// 	for i := 0; i < len(hashes); i++ {
// 	// 		if hashes[i] > blockHash {
// 	// 			responsibleServer = serverMap[hashes[i]]
// 	// 			break
// 	// 		}
// 	// 	}
// 	// 	if responsibleServer == "" {
// 	// 		responsibleServer = serverMap[hashes[0]]
// 	// 	}
// 		// fmt.Println(dat, responsibleServer)
// 		currMap := blockStoreMap[responsibleServer].Hashes
// 		blockStoreMap[responsibleServer].Hashes = append(currMap, blockHash)
// 	}
// 	return &BlockStoreMap{BlockStoreMap: blockStoreMap}
// }
