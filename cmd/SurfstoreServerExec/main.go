package main

import (
	"cse224/proj5/pkg/surfstore"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Usage String
const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\n")
	}

	// Parse command-line argument flags
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	// TODO: change your code in cmd/SurfstoreServerExec/main.go to handle multiple tail aruguments.
	// In previous project, we only have one tail argument indicating the single block server's address.
	// Now we want to handle multiple arguments to configure multiple block servers.

	// Use tail arguments to hold BlockStore address
	args := flag.Args()
	// blockStoreAddr := ""
	blockStoreAddr := args
	// if len(args) == 1 {
	// 	blockStoreAddr = args[0]
	// }

	// Valid service type argument
	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	// Add localhost if necessary
	addr := ""
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(io.Discard)
	}

	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddr))
}

func startServer(hostAddr string, serviceType string, blockStoreAddrs []string) error {
	// Create a new gRPC server
	server := grpc.NewServer()

	// Register services based on the serviceType
	switch serviceType {
	case "both":
		// Register MetaStore service
		metaStore := surfstore.NewMetaStore(blockStoreAddrs)
		surfstore.RegisterMetaStoreServer(server, metaStore)
		// Register BlockStore service
		blockStore := surfstore.NewBlockStore()
		surfstore.RegisterBlockStoreServer(server, blockStore)

	case "meta":
		// Register MetaStore service only
		metaStore := surfstore.NewMetaStore(blockStoreAddrs)
		surfstore.RegisterMetaStoreServer(server, metaStore)

	case "block":
		// Register BlockStore service only
		blockStore := surfstore.NewBlockStore()
		surfstore.RegisterBlockStoreServer(server, blockStore)

	default:
		return fmt.Errorf("invalid service type: %s", serviceType)
	}

	// Enable reflection for the gRPC server
	reflection.Register(server)

	// Start listening on the specified host address
	lis, err := net.Listen("tcp", hostAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", hostAddr, err)
	}

	// fmt.Printf("Starting gRPC server on %s with service type: %s", hostAddr, serviceType)
	if err := server.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve gRPC server: %v", err)
	}
	return nil
}
