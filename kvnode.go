package main

import (
	"./block"
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"time"
	//"errors"
	"strings"
	"unsafe"
)

const HEARTBEAT_TIMEOUT = 3 * time.Second
const TIME_BETWEEN_HEARTBEATS = time.Millisecond
const NODE_DIAL_TIMEOUT = 6 * time.Second

// Server for kvservice
type KVServer int

// Server for nodes
type NServer int

// Represents a key in the system.
type Key string

// Represent a value in the system.
type Value string

// KVServer.Get
type GetRequest struct {
	Key           Key
	TransactionId int
}
type GetResponse struct {
	Value   Value
	Success bool
}

// KVServer.Put
type PutRequest struct {
	Key           Key
	Value         Value
	TransactionId int
}
type PutResponse struct {
	Success bool
}

// KVServer.Commit
type CommitRequest struct {
	TransactionId int
	ValidateNum   int
}
type CommitResponse struct {
	Success bool
	TxnID int
}

// KVServer.Abort
type AbortRequest struct {
	TransactionId int
}
type AbortResponse struct{}

// KVServer.CreateTransaction
type CreateTransactionRequest struct {
	IpPort string
}
type CreateTransactionResponse struct {
	TransactionId int
}

type GetChildrenRequest struct {
	PrevHash string
}
type GetChildrenResponse struct {
	Children []string
}

type BroadcastBlockRequest struct {
	//Block blk.Block
	PrevHash      []byte
	TransactionID int
	NodeID        int
	Nonce         uint32
	BlockType     int
	SelfHash      []byte
	IsValidated   bool
	//Test string
}

type BroadcastBlockResponse struct {
	Validated bool
}

// KVServer.Ping or NServer.Ping or CServer.Ping
type PingRequest struct {
	ID int // ID is currently only used to confirm that client with transaction id = ID is still alive
}
type PingResponse struct{}

type Entry struct {
	Key   Key
	Value Value
}

type Transaction struct {
	TransactionID  int
	LocalStore     map[Key]Value // Local copy of the KVStore to work on for a txn
	ValuesReplaced []Entry       // List of all values transaction has replaces - to determine conflicts

}

var (
	GHash            []byte        // Genesis block hash value
	NumZeroes        int           // Number of leading zeroes to check for in proof of work
	ListOfNodes      []string      // List of ip:ports of nodes
	NodeID           int           // Node ID of the current node
	ListenNodeIn     string        // Ip:port to listen on for incoming node connections
	ListenClientIn   string        // Ip:port to listen on for incoming client connections
	KVStore          map[Key]Value // Master KV store
	TransactionQueue []int         // queue to store and process pending transactions
	AbortedTransactions []int      // keeps a list of aborted transactions

	TransactionsMap map[int]Transaction

	livingNodeIPs        []string   // array of living nodes
	largestTransactionId int        // Globally Unique Transaction Id
	operationMutex       sync.Mutex // ensures only one thing happens at a time on this node
	Counter              = 2        // Length of block chain should Counter

	BlockChain *blk.Block          // First Block in the tree, also known as the genesis block
)

func main() {
	if len(os.Args) != 7 {
		panic("Usage: go run kvnode.go [ghash] [num-zeroes] [nodesFile] [nodeID] [listen-node-in IP:port]" +
			" [listen-client-in IP:port]")
	}

	operationMutex.Lock() // don't allow any other methods to run while initializing

	parseArguments()

	// setup rpc servers
	go setupKVServer(ListenClientIn)
	go setupNodeServer(ListenNodeIn)
	go listenForClientHeartbeatSetup(ListenClientIn)

	// setup heartbeat
	livingNodeIPs = ListOfNodes
	for _, nodeIp := range livingNodeIPs {
		setupHeartbeatToNode(nodeIp)
	}

	// setting up queue so we can process one transaction block request at a time
	TransactionQueue = []int{}

	// keep args the same for all nodes, so the genesis block is the same for all
	//selfHash, nonce := blk.Hashing(GHash, 1, 1, 1, NumZeroes)
	firstBlock := blk.Block{
		PrevHash:      nil,
		TransactionID: 0,
		NodeID:        0,
		Nonce:         0,
		ChildBlocks:   nil,
		BlockType:     0,
		ParentBlock:   nil,
		SelfHash:      GHash,
	}
	// Create the Block chain starting with the genesis block
	BlockChain = &firstBlock
	debugLog("First block in BlockChain (Genesis):")
	printBlock(BlockChain)

	// Initialize maps
	KVStore = make(map[Key]Value)
	TransactionsMap = make(map[int]Transaction)

	// start making blocks here
	go ProcessTransactionQueue()

	// done initializing
	operationMutex.Unlock()

	debugLog("Finished initializing. Waiting forever...")
	<-make(chan int, 1)
}

// Simple Get method - returns val from local txn KeyVal store
func (k *KVServer) Get(req GetRequest, res *GetResponse) (err error) {
	debugLog("Get: ", req.Key)
	txn := TransactionsMap[req.TransactionId]
	val := txn.LocalStore[req.Key]
	debugLog("Val: ", val)
	res.Value = val
	res.Success = true
	return
}

// Simple put method
func (k *KVServer) Put(req PutRequest, res *PutResponse) (err error) {
	debugLog("Put: ", req.Key, req.Value)
	txn := TransactionsMap[req.TransactionId]
	txn.LocalStore[req.Key] = req.Value
	res.Success = true
	return
}

// Simple commit method
func (k *KVServer) Commit(req CommitRequest, res *CommitResponse) (err error) {

	AddTransactionToQueue(req.TransactionId)

	validateNum := req.ValidateNum
	txn := TransactionsMap[req.TransactionId]
	debugLog("Commit: ", txn)
	isConflict := false
	for _, entry := range txn.ValuesReplaced {
		if entry.Value != KVStore[entry.Key] {
			isConflict = true
		}
	}
	if isConflict {
		// There is a key conflict, so abort the transaction
		res.Success = false
	} else {
		// When block is appended to block chain, check to see if validateNum blocks can be committed
		KVStore = txn.LocalStore
		res.Success = true
	}

	txnBlock := SearchChain(req.TransactionId, BlockChain)
	// Wait until the block is added to the chain before moving forward
	for txnBlock == nil {
		txnBlock = SearchChain(req.TransactionId, BlockChain)
		//debugLog("TxnBlock in loop is: ", txnBlock)
	}
	// Block here until the commit has been validated
	WaitForBlockValidation(txnBlock, validateNum)
	delete(TransactionsMap, txn.TransactionID)

	// Block is validated so you are in the longest chain (hopefully) so just inc the txn ID
	txnBlock.TransactionID = largestTransactionId
	largestTransactionId++
	res.TxnID = txnBlock.TransactionID
	debugLog("commit after being validate")
	return
}

func (k *KVServer) Abort(req AbortRequest, res *AbortResponse) (err error) {
	delete(TransactionsMap, req.TransactionId)
	return
}

func (k *KVServer) CreateTransaction(req CreateTransactionRequest, res *CreateTransactionResponse) (err error) {
	debugLog("Creating Transaction with ID:", largestTransactionId)
	stringIP := strings.Replace(req.IpPort, ".", "", -1)
	stringIP += strconv.Itoa(largestTransactionId)
	intIP, err := strconv.Atoi(stringIP)
	txn := Transaction{
		TransactionID:  intIP,
		LocalStore:     KVStore,
		ValuesReplaced: nil,
	}
	TransactionsMap[txn.TransactionID] = txn
	largestTransactionId++

	res.TransactionId = txn.TransactionID
	return
}

// Return value should be the hash values of all of the children blocks that have the block identified by parentHash
// as their prev-hash value.
func (k *KVServer) GetChildren(req GetChildrenRequest, res *GetChildrenResponse) (err error) {
	fmt.Println("In get children")
	fmt.Println("hash is: ", req.PrevHash)
	var children []string
	if strings.Compare(req.PrevHash, "") == 0 {
		// return first element in the tree
		fmt.Println("In first statement")
		children = append(children, toString(BlockChain.SelfHash))
	} else {
		fmt.Println("In else")
		block := SearchChainSelfHash(req.PrevHash, BlockChain)
		fmt.Println("Block from self hash: ", block)
		if block == nil {
			// Block does not exist so return
			return
		}
		for _, block := range block.ChildBlocks {
			children = append(children, toString(block.SelfHash))
		}
	}
	res.Children = children
	return
}

// Broadcast block to all other nodes
func (*NServer) BroadcastBlock(request BroadcastBlockRequest, reply *BroadcastBlockResponse) error {
	debugLog("received block from other node")
	block := blk.Block{
		PrevHash:      request.PrevHash,
		TransactionID: request.TransactionID,
		NodeID:        request.NodeID,
		Nonce:         request.Nonce,
		ChildBlocks:   nil,
		BlockType:     request.BlockType,
		ParentBlock:   nil,
		SelfHash:      request.SelfHash,
		IsValidated:   request.IsValidated,
	}
	sblock := SearchChainSelfHash(toString(request.SelfHash), BlockChain)
	if sblock == nil {
		reply.Validated = blk.ValidateBlock(block, NumZeroes)
	} else {
		reply.Validated = true
	}
	return nil
}

// broadcast blocks to other nodes
func BroadcastBlockToOtherNodes(block *blk.Block) bool {
	debugLog("Sending block to other nodes")
	request := BroadcastBlockRequest{
		PrevHash:      block.PrevHash,
		NodeID:        block.NodeID,
		Nonce:         block.Nonce,
		TransactionID: block.TransactionID,
		BlockType:     block.BlockType,
		SelfHash:      block.SelfHash,
		IsValidated:   block.IsValidated,
	}
	var response BroadcastBlockResponse
	for _, ipPort := range livingNodeIPs {
		debugLog("trying to hit node: ", ipPort)
		client, err := rpc.Dial("tcp", ipPort)
		checkError(err, false, "")
		err = client.Call("NServer.BroadcastBlock", request, &response)
		debugLog("validate response:", response.Validated)
		//if !response.Validated {
		// do nothing, since the block will not be appended
		//}
	}

	return response.Validated
}

// add block to the longest block chain
func AddBlock(block *blk.Block) {
	debugLog("In AddBlock")
	//debugLog("Block im going to add: ", block)
	lastBlock := blk.GetLastBlockOfLongestBlockChain(BlockChain)
	validateCheck := true
	if len(livingNodeIPs) > 0 {
		validateCheck = BroadcastBlockToOtherNodes(block)
	}
	bsh := toString(block.SelfHash)
	duplicateBlock := SearchChainSelfHash(bsh, BlockChain)
	prevhashBlockCheck := SearchChainHash(toString(block.PrevHash), BlockChain)

	// million dollars code
	if block.BlockType == 0 {
		debugLog("duplicate block is: ", duplicateBlock)
		debugLog("validatecheck is: ", validateCheck)
		if duplicateBlock == nil && validateCheck {
			debugLog("prevhas block : ", prevhashBlockCheck)
			if prevhashBlockCheck != nil {
				debugLog("prevhashBlockCheck is not nil")
				// race condition block from other nodes, we will pick the one with smallest node id
				if lastBlock.NodeID < block.NodeID {
					debugLog("lastBlock.NodeID < block.NodeID", "lastBlock.NodeID",
						lastBlock.NodeID, "block.NodeID",block.NodeID)
					// do nothing
					// smaller node id we keep the last block in the chain
				} else {
					debugLog("lastBlock.NodeID > block.NodeID", "lastBlock.NodeID",
						lastBlock.NodeID, "block.NodeID",block.NodeID)
					// we replace the block here
					parentBlockOfLastBlock := lastBlock.ParentBlock
					//lastBlock.ParentBlock == nil
					setNilPtr(unsafe.Pointer(&lastBlock.ParentBlock))
					debugLog("lastBlock.ParentBlock == nil",lastBlock.ParentBlock)
					block.ParentBlock = parentBlockOfLastBlock
					for key, value:= range parentBlockOfLastBlock.ChildBlocks {
						if value == lastBlock {
							setNilPtr(unsafe.Pointer(&value))
						}
						debugLog("key", key, "value", value)
					}
					// check if transaction was aborted before adding to chain
					if !TransactionWasAborted(block.TransactionID) {
						parentBlockOfLastBlock.ChildBlocks = append(
							parentBlockOfLastBlock.ChildBlocks, block)
					}

				}
			} else {
				debugLog("else :::::::")
				// check if transaction was aborted before adding to chain
				if !TransactionWasAborted(block.TransactionID) {
					debugLog("Add block to chain")
					lastBlock.ChildBlocks = append(lastBlock.ChildBlocks, block)
				}
			}

		}
	} else {
		// transaction block
		if duplicateBlock == nil && validateCheck {
			// check if transaction was aborted before adding to chain
			if !TransactionWasAborted(block.TransactionID) {
				debugLog("Add block to chain")
				lastBlock.ChildBlocks = append(lastBlock.ChildBlocks, block)
			}
		}
	}

}

func TransactionWasAborted(transactionid int) bool  {
	for _, tid := range AbortedTransactions {
		if transactionid == tid {
			return true
		}
	}
	return false
}

func setNilPtr(p unsafe.Pointer) {
	*(**int)(p) = nil
}

// Wait for the txn block to be validated
func WaitForBlockValidation(txnBlock *blk.Block, validateNum int) {
	debugLog("WaitForBlockValidation")
	tmp := txnBlock
	var blocksFollowing = 0
	debugLog("validateNum is: ", validateNum)
	debugLog("Block validate is: ", txnBlock.IsValidated)
	//Don't return to client until the block is validated

	for !txnBlock.IsValidated {
		// TODO does not work with forking
		for len(tmp.ChildBlocks) > 0 && tmp.ChildBlocks[0] != nil {
			//debugLog("enter loop again with new info")
			if blocksFollowing >= validateNum {
				//debugLog("blocksFollowing >= validateNublocksFollowing >= validateNu")
				txnBlock.IsValidated = true
				PrintChain(txnBlock)
				break
			}
			// only one child
			if len(tmp.ChildBlocks) == 1 {
				//debugLog("Blocks following is: ", blocksFollowing)
				blocksFollowing++
				//debugLog("one child")
				tmp = tmp.ChildBlocks[0]
			} else {
				//debugLog("multiple child")
				// forking with multiple child
				for _, child := range tmp.ChildBlocks {
					debugLog("multiple child", child)
				}

			}
		}
		tmp = txnBlock
		blocksFollowing = 0
		time.Sleep(200 * time.Millisecond)
		}
	return
}

// add to transaction queue
func AddTransactionToQueue(transactionid int) {
	debugLog("Add Transaction to queue")
	TransactionQueue = append(TransactionQueue, transactionid)
}

// queues the transaction request and process them.
func ProcessTransactionQueue() {
	debugLog("Process transcation queue")
	//PrintChain(BlockChain)
	var block *blk.Block

	for true {
		debugLog("Executing for loop")
		if len(TransactionQueue) > 0 && TransactionQueue[0] != 0 {
			debugLog("MineTransactionBlock")
			debugLog("Queue before delete: ", TransactionQueue)
			transactionToBeProccessed := TransactionQueue[0]
			TransactionQueue = TransactionQueue[1:]
			debugLog("Queue is now: ", TransactionQueue)
			block = blk.MineTransactionBlock(transactionToBeProccessed, NodeID, BlockChain, NumZeroes)
		} else {
			debugLog("MineNoopBlock")
			block = blk.MineNoopBlock(NodeID, BlockChain, NumZeroes)
		}

		AddBlock(block)
		debugLog("Blockchain for counter is: ", Counter)
		PrintChain(BlockChain)
		Counter++
	}

}

// Search for the block in the chain with TxnID == tid
func SearchChain(tid int, block *blk.Block) *blk.Block {
	if block.TransactionID == tid {
		return block
	}
	if block.ChildBlocks != nil {
		if len(block.ChildBlocks) == 1 {
			return SearchChain(tid, block.ChildBlocks[0])
		} else {
			for _, child := range block.ChildBlocks {
				return SearchChain(tid, child)
			}
		}
	} else {
		//debugLog("Did not find block")
	}
	return nil
}

// Search for the block in the chain with block.PrevHash == prevHash
func SearchChainHash(prevHash string, block *blk.Block) *blk.Block {
	if strings.Compare(toString(block.PrevHash), prevHash) == 0 {
		return block
	}
	if block.ChildBlocks != nil {
		if len(block.ChildBlocks) == 1 {
			return SearchChainHash(prevHash, block.ChildBlocks[0])
		} else {
			for _, child := range block.ChildBlocks {
				return SearchChainHash(prevHash, child)
			}
		}
	} else {
		debugLog("Did not find block")
	}
	return nil
}

// Search for the block in the chain with block.selfhash == selfhash
func SearchChainSelfHash(selfhash string, block *blk.Block) *blk.Block {
	//debugLog("deuplicate block check")
	if strings.Compare(selfhash, toString(block.SelfHash)) == 0 {
		return block
	}
	if block.ChildBlocks != nil {
		if len(block.ChildBlocks) == 1 {
			return SearchChainSelfHash(selfhash, block.ChildBlocks[0])
		} else {
			for _, child := range block.ChildBlocks {
				return SearchChainSelfHash(selfhash, child)
			}
		}
	} else {
		debugLog("Did not find block")
	}
	return nil
}

// Search for the block in the chain with TxnID == tid
func PrintChain(block *blk.Block) {
	for len(block.ChildBlocks) != 0 {
		printBlock(block)
		time.Sleep(500 * time.Millisecond)
		block = block.ChildBlocks[0]
	}
	printBlock(block)
}

// create an rpc server for clients
func setupKVServer(ipPort string) {
	debugLog("Setting up kv server for clients at", ipPort)
	kvServer := rpc.NewServer()
	p := new(KVServer)
	kvServer.Register(p)

	l, err := net.Listen("tcp", ipPort)
	checkError(err, true, "listening for connection")
	for {
		conn, err := l.Accept()
		//debugLog("Accepted kv server connection")
		checkError(err, false, "accepting connection")
		go kvServer.ServeConn(conn)
	}
}

//// create an rpc server for clients
func setupNodeServer(ipPort string) {
	debugLog("Setting up node server at", ipPort)
	nServer := rpc.NewServer()
	w := new(NServer)
	nServer.Register(w)

	l, err := net.Listen("tcp", ipPort)
	checkError(err, true, "listening for connection")
	for {
		conn, err := l.Accept()
		//debugLog("Accepted node server connection")
		checkError(err, false, "accepting connection")
		go nServer.ServeConn(conn)
	}
}

//// Initialization functions

// sets up a heartbeat to a node
func setupHeartbeatToNode(nodeIpPort string) {
	goodClientChannel := make(chan *rpc.Client, 1)
	quitDialingChannel := make(chan bool, 1)

	// keep trying to dial until time limit
	go func() {
		for {
			select {
			case <-quitDialingChannel:
				debugLog("Stopping dialing to node: ", nodeIpPort)
				return
			default:
				// keep trying to dial a client
				client, err := rpc.Dial("tcp", nodeIpPort)
				checkError(err, false, "Dialing to node: ", nodeIpPort,
					" failed while initializing")
				if err == nil {
					// made a good client
					goodClientChannel <- client
					return
				}
			}
		}
	}()

	var client *rpc.Client
	select {
	case client = <-goodClientChannel:
		debugLog("Connected to node:", nodeIpPort)
	case <-time.After(NODE_DIAL_TIMEOUT):
		debugLog("Timed out waiting for dial to node: ", nodeIpPort)
		quitDialingChannel <- true
		nodeDied(nodeIpPort, false)
		return
	}

	go func() {
		heartbeatChannel := make(chan error, 1)
		heartbeatFunc := func() {
			heartbeatChannel <- client.Call("NServer.Ping", PingRequest{}, &PingResponse{})
		}

	Loop:
		for {
			go heartbeatFunc()
			select {
			case err := <-heartbeatChannel:
				checkError(err, false, "Got error when heartbeating to ", nodeIpPort)
				if err != nil {
					// error, so assume it's down
					break Loop
				}
				// if we made it here, we want to keep repeating the heartbeat
				//debugLog("Successful heartbeat to node: ", nodeIpPort)

			case <-time.After(HEARTBEAT_TIMEOUT):
				break Loop
			}

			time.Sleep(TIME_BETWEEN_HEARTBEATS)
		}

		// got here due to reaching heartbeat timeout or error in heartbeat
		nodeDied(nodeIpPort, true)
	}()
}

// Handle a node death
// Set getOperationMutexLock to false if you've already locked it
func nodeDied(nodeIpPort string, getOperationMutexLock bool) {
	if getOperationMutexLock {
		operationMutex.Lock()
		defer operationMutex.Unlock()
	}
	debugLog("Marking node: ", nodeIpPort, " as dead")
	// remove dead node from alive nodes
	nodeIndex := -1
	for index, nip := range livingNodeIPs {
		if nip == nodeIpPort {
			nodeIndex = index
		}
	}

	if nodeIndex < 0 {
		debugLog(nodeIpPort, " was already deleted")
		return // already deleted
	}

	livingNodeIPs = append(livingNodeIPs[:nodeIndex], livingNodeIPs[nodeIndex+1:]...)
}

// Parse command line args
func parseArguments() {
	args := os.Args[1:]

	GHash = []byte(args[0])

	numZeroes, err := strconv.Atoi(args[1])
	checkError(err, false, "")
	NumZeroes = numZeroes

	// Leave one space in front of array to match with node Id
	ListOfNodes = parseNodeFile(args[2])
	fmt.Println("Nodes: ", ListOfNodes)

	// setting node id
	NodeID, err = strconv.Atoi(args[3])
	checkError(err, false, err)
	debugLog("NodeID in parsearguments", NodeID)

	// set up values for unique transaction ids on each node
	largestTransactionId = 1

	// remove self from living ips
	ListOfNodes = append(ListOfNodes[0:NodeID-1], ListOfNodes[NodeID:]...)

	debugLog("NodeFile without me is: ", ListOfNodes)

	// sort ips to get same order as client
	sort.Strings(ListOfNodes)

	// Ip:Port to listen on for incoming node connections
	ListenNodeIn = args[4]

	// Ip:Port to listen on for incoming client connections
	ListenClientIn = args[5]
}

// Parse the nodeFile and return an array of node Ip:Port's
func parseNodeFile(nodeFile string) []string {
	file, err := os.Open(nodeFile)
	if err != nil {
		checkError(err, false, "Error while parsing file: ")
	}

	nodes := make([]string, 0)
	scanner := bufio.NewScanner(file)

	// Scan the file line by line and add it to the list of nodes
	for scanner.Scan() {
		nodeIpPort := scanner.Text()
		nodes = append(nodes, nodeIpPort)
	}
	file.Close()
	// Contains the Ip:Ports of the node Ip:Port's
	return nodes
}

// RPC KVServer.Ping
func (*KVServer) Ping(request PingRequest, reply *PingResponse) error {
	return nil
}

// RPC NServer.Ping
func (*NServer) Ping(request PingRequest, reply *PingResponse) error {
	//debugLog("Got Ping request", PingRequest{})
	return nil
}

// create a udp listener for clients. when client dies, abort this transaction
func listenForClientHeartbeatSetup(ipPort string) {
	udpAddr, err := net.ResolveUDPAddr("udp", ipPort)
	checkError(err, true, "create udp conn")
	udpConn, err := net.ListenUDP("udp", udpAddr)
	checkError(err, true, "listen udp")

	lastHeartbeatSetupTx := -2
	for {
		buf := make([]byte, 1)
		_, remoteAddr, err := udpConn.ReadFrom(buf)
		checkError(err, false, "reading from new worker")
		if err != nil {
			continue
		}

		remoteUdpAddr := remoteAddr.(*net.UDPAddr)
		txToHeartbeatFor := int(buf[0])

		if txToHeartbeatFor != lastHeartbeatSetupTx {
			lastHeartbeatSetupTx = txToHeartbeatFor
			go setupHeartbeatToClient(remoteUdpAddr.String(), txToHeartbeatFor)
		}
	}
}


func setupHeartbeatToClient(clientIpPort string, tx int) {
	debugLog("Got heartbeat setup for tx: ", tx)

	// get rpc client for heartbeating
	client, err := rpc.Dial("tcp", clientIpPort)
	checkError(err, false, "dialing client for heartbeat")
	if err != nil {
		operationMutex.Lock()
		debugLog("Could not connect to client for transaction, ", tx, ". Aborting the transaction...")
		abortTransaction(tx)
		operationMutex.Unlock()
		return
	}

	// heartbeat to client
	resultChannel := make(chan error, 1)
	heartbeatFunc := func() {
		request := PingRequest{tx}
		var reply PingResponse
		resultChannel <- client.Call("LibServer.Ping", request, &reply)
	}

	Loop:
	for {
		heartbeatFunc()
		select {
		case err := <-resultChannel:
		//debugLog("Pinging client for tx: ", tx, " got err result: ", err)
			checkError(err, false, "Pinging client for result")
			if err != nil {
				break Loop
			}
		case <-time.After(HEARTBEAT_TIMEOUT):
			debugLog("Heartbeat timeout to client of transaction, ", tx)
			break Loop
		}

		time.Sleep(TIME_BETWEEN_HEARTBEATS)
	}

	// client d/c means transaction should be aborted
	operationMutex.Lock()
	debugLog("Client for transaction, ", tx, ", disconnected. Aborting the transaction if it's still in progress...")
	abortTransaction(tx)
	operationMutex.Unlock()
}

// helper function for aborting a transaction
func abortTransaction(transactionId int) {

	// add to be abort transaction id into the AbortTransaction Array, this array will be checked before append the
	// transaction block to the chain
	AbortedTransactions = append(AbortedTransactions, transactionId)

	abortedTxIndex := -1


	// transaction is done if not in progress
	if abortedTxIndex < 0 {
		debugLog("Tried to abort transaction that is already completed: ", transactionId)
		return
	}

	// remove from TransactionQueue
	TransactionQueue = append(TransactionQueue[:abortedTxIndex], TransactionQueue[abortedTxIndex+1:]...)

}




//// Utility functions

// Returns the string representation of a byte array (or hash)
func toString(b []byte) string {
	s := bytes.NewBuffer(b).String()
	return s
}

// Return the IP address from IP:Port
func getIP(ipPort string) string {
	address := ipPort
	var colon int
	for i := 0; i < len(address); i++ {
		if strings.Compare(string(address[i]), ":") == 0 {
			colon = i
			break
		}
	}
	workerIP := address[:colon]
	return workerIP
}

// logging that can be disabled
func debugLog(v ...interface{}) {
	fmt.Println(v...)
}

func printBlock(block *blk.Block) {
	if block.ParentBlock != nil {
		fmt.Println("TransactionID:", block.TransactionID, "NodeID:", block.NodeID, "Nonce:", block.Nonce,
			"BlockType:", block.BlockType, "IsValidated:", block.IsValidated, "ChildBlocks:", block.ChildBlocks,
			"ParentBlock:", block.ParentBlock.Nonce, "\n")
	} else {
		fmt.Println("TransactionID:", block.TransactionID, "NodeID:", block.NodeID, "Nonce:", block.Nonce,
			"BlockType:", block.BlockType, "IsValidated:", block.IsValidated, "ChildBlocks:", block.ChildBlocks,
			"ParentBlock:", nil, "\n")
	}

}

// error handling
func checkError(err error, exit bool, msgs ...interface{}) {
	msg := fmt.Sprint(msgs...)
	if err != nil {
		if len(msg) > 0 {
			msg = msg + ":"
		}

		if exit {
			log.Println(msg, err)
			debug.PrintStack()
			os.Exit(-1)
		} else {
			debugLog(msg, err)
		}
	}
}
