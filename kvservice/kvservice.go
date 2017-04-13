/*

This package specifies the application's interface to the key-value
service library to be used in assignment 7 of UBC CS 416 2016 W2.

*/

package kvservice

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
	"strconv"
)

const CREATE_CLIENT_TIMEOUT = 6 * time.Second
const DIAL_TIMEOUT = 2 * time.Second

var transactionAlreadyCompletedError = errors.New(
	"Transaction was aborted or is already committed")

// Represents a key in the system.
type Key string

// Represent a value in the system.
type Value string

// Server for kvservice
type KVServer int

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

// KVServer.Abort
type AbortRequest struct {
	TransactionId int
}
type AbortResponse struct{}

var (
	clients    []*rpc.Client
	nodesToTry []string
	//transactionId        int
	transactionIsAborted bool
	OwnIp                string
)

// An interface representing a connection to the key-value store. To
// create a new connection use the NewConnection() method.
type connection interface {
	// The 'constructor' for a new logical transaction object. This is the
	// only way to create a new transaction. The returned transaction must
	// correspond to a specific, reachable, node in the k-v service. If
	// none of the nodes are reachable then tx must be nil and error must
	// be set (non-nil).
	NewTX() (newTX tx, err error)

	// Used by a client to ask a node for information about the
	// block-chain. Node is an IP:port string of one of the nodes that
	// was used to create the connection.  parentHash is either an
	// empty string to indicate that the client wants to retrieve the
	// SHA 256 hash of the genesis block. Or, parentHash is a string
	// identifying the hexadecimal SHA 256 hash of one of the blocks
	// in the block-chain. In this case the return value should be the
	// string representations of SHA 256 hash values of all of the
	// children blocks that have the block identified by parentHash as
	// their prev-hash value.
	GetChildren(node string, parentHash string) (children []string)

	// Close the connection.
	Close()
}

type myconn struct{}

// Concrete implementation of a tx interface.
type mytx struct {
	TransactionID int
}

// KVServer.Ping or NServer.Ping or CServer.Ping
type PingRequest struct {
	ID int // ID is currently only used to confirm that client with transaction id = ID is still alive
}
type PingResponse struct{}

// An interface representing a client's transaction. To create a new
// transaction use the connection.NewTX() method.
type tx interface {
	// Retrieves a value v associated with a key k as part of this
	// transaction. If success is true then v contains the value
	// associated with k and err is nil. If success is false then the
	// tx has aborted, v is an empty string, and err is non-nil. If
	// success is false, then all future calls on this transaction
	// must immediately return success = false (indicating an earlier
	// abort).
	Get(k Key) (success bool, v Value, err error)

	// Associates a value v with a key k as part of this
	// transaction. If success is true, then put was recoded
	// successfully, otherwise the transaction has aborted (see
	// above).
	Put(k Key, v Value) (success bool, err error)

	// Commits this transaction. If success is true then commit
	// succeeded, otherwise the transaction has aborted (see above).
	// The validateNum argument indicates the number of blocks that
	// must follow this transaction's block in the block-chain along
	// the longest path before the commit returns with a success.
	// txID represents the transactions's global sequence number
	// (which determines this transaction's position in the serialized
	// sequence of all the other transactions executed by the
	// service).
	Commit(validateNum int) (success bool, txID int, err error)

	// Aborts this transaction. This call always succeeds.
	Abort()
}

// The 'constructor' for a new logical connection object. This is the
// only way to create a new connection. Takes a set of k-v service
// node ip:port strings.
func NewConnection(nodes []string) connection {
	//fmt.Println("NewConnection")

	nodesToTry = nodes
	//fmt.Println("Conn nodes: ", nodesToTry)

	// ensure everyone tries connecting to nodes in the same order
	// guaranteed to be subset of nodesFile on node https://piazza.com/class/ixc8krzqchx5p4?cid=440
	sort.Strings(nodesToTry)

	return &myconn{}
}

// Create a new transaction.
func (conn *myconn) NewTX() (tx, error) {
	////fmt.Println("NewTX")

	// cleanup old transaction variables
	if clients != nil {
		for _, c := range clients {
			c.Close()
		}
		clients = nil
	}
	transactionIsAborted = false
	//transactionId = -1
	////fmt.Println("Nodes to try: ", nodesToTry)
	for len(nodesToTry) > 0 {
		//fmt.Println("Trying nodes")
		createRpcClientForTransaction()
		if clients == nil {
			//fmt.Println("Client was nil after creating, so all nodes are dead")
			break // no more nodes to try
		}
		OwnIp = getAddress()
		var response CreateTransactionResponse
		request := CreateTransactionRequest{
			IpPort: OwnIp,
		}
		//fmt.Println("all clients: ", clients)
		//fmt.Println("Before call")
		for _, client := range clients {
			err := client.Call("KVServer.CreateTransaction", request, &response)
			CheckError("", err, false)
		}
		return &mytx{TransactionID: response.TransactionId}, nil
	}

	return nil, errors.New("All nodes are dead")
}

// Ask node for the hash values of blocks pointing to the block with the hash parentHash
func (conn *myconn) GetChildren(node string, parentHash string) (children []string) {
	var response GetChildrenResponse
	request := GetChildrenRequest{PrevHash: parentHash}

	tcpClient, err := net.DialTimeout("tcp", node, DIAL_TIMEOUT)
	CheckError("dialing client", err, false)
	//fmt.Println("client is: ", tcpClient)
	//// make a test rpc call to ensure client works
	var pingResponse PingResponse
	pingRequest := PingRequest{}
	client := rpc.NewClient(tcpClient)

	err1 := client.Call("KVServer.Ping", pingRequest, &pingResponse)
	CheckError("pinging node", err1, false)

	err2 := client.Call("KVServer.GetChildren", request, &response)
	CheckError("", err2, false)

	client.Close()

	return response.Children
}

// Close the connection.
func (conn *myconn) Close() {
	//fmt.Println("Close")
	// close rpc connections
	if clients != nil {
		for _, c := range clients {
			c.Close()
		}
	}
	// let any ongoing transactions get killed by heartbeat
	//transactionId = -1
}

// tries to create a new rpc client for a transaction.
// sets client to nil if no nodes are alive
func createRpcClientForTransaction() {
	// making rpc connection to the first node
	clients = nil

	doneChannel := make(chan bool, 1)
	var connectedNode string
	nodesLeft := len(nodesToTry)
	index := 0
	// checking dead node for 3secs timeout when establishing connection
	//Loop:
	for len(nodesToTry) > 0 && nodesLeft > 0 {
		quitTryingToConnect := make(chan bool, 1)
		go func(node string) {
			for {
				select {
				case <-quitTryingToConnect:
					//fmt.Println("Got signal to quit trying to connect to node: ", node)
					return
				default:
					// keep trying to connect to client until we get quit signal
					c, err := rpc.Dial("tcp", node)
					//CheckError("dialing client", err, false)
					if err != nil {
						continue
					}
					// make a test rpc call to ensure client works
					var response PingResponse
					request := PingRequest{}
					err = c.Call("KVServer.Ping", request, &response)
					CheckError("pinging node", err, false)
					if err == nil {
						clients = append(clients, c)
						connectedNode = node
						doneChannel <- true
						return
					}
				}
			}
		}(nodesToTry[index])

		// wait while trying to connect
		select {
		case <-doneChannel:
			//fmt.Println("Connected to node: ", connectedNode)
			nodesLeft--
			index++
			//break Loop
		case <-time.After(CREATE_CLIENT_TIMEOUT):
			fmt.Println("Timeout connecting to node: ", connectedNode)
			quitTryingToConnect <- true
			nodesToTry = nodesToTry[1:] // pop the front node
			nodesLeft--
		}
	}
}

// Retrieves a value v associated with a key k.
func (t *mytx) Get(k Key) (success bool, v Value, err error) {
	// quit when transaction is already dead
	//fmt.Println("In get")
	if transactionIsAborted {
		return false, v, transactionAlreadyCompletedError
	}
	//fmt.Println("Get")
	var response GetResponse
	request := GetRequest{
		Key:           k,
		TransactionId: t.TransactionID,
	}
	answers := make([]string, 0)
	allSuccessful := true
	// Send request to every node
	for _, client := range clients {
		err = client.Call("KVServer.Get", request, &response)
		if err != nil || response.Success == false {
			transactionIsAborted = true
			err = transactionAlreadyCompletedError
			v = ""
			response.Success = false
			allSuccessful = false
		} else {
			answers = append(answers, string(response.Value))
		}
	}
	if !allSuccessful {
		response.Success = false
	}
	mostCommon := getMostCommon(answers)
	return response.Success, Value(mostCommon), err
}

// Associates a value v with a key k.
func (t *mytx) Put(k Key, v Value) (success bool, err error) {
	// quit when transaction is already dead
	if transactionIsAborted {
		return false, transactionAlreadyCompletedError
	}
	//fmt.Println("Put")
	var response PutResponse
	request := PutRequest{
		Key:           k,
		Value:         v,
		TransactionId: t.TransactionID,
	}
	allSuccessful := true
	for _, client := range clients {
		err = client.Call("KVServer.Put", request, &response)
		CheckError("", err, false)
		if err != nil || response.Success == false {
			transactionIsAborted = true
			err = transactionAlreadyCompletedError
			response.Success = false
			allSuccessful = false
		}
	}
	if !allSuccessful {
		response.Success = false
	}
	return response.Success, err
}

// Commits the transaction.
func (t *mytx) Commit(validateNum int) (success bool, txID int, err error) {
	// quit when transaction is already dead
	if transactionIsAborted {
		return false, t.TransactionID, transactionAlreadyCompletedError
	}
	//fmt.Println("Commit")

	var response CommitResponse
	request := CommitRequest{TransactionId: t.TransactionID, ValidateNum: validateNum}
	allSuccessful := true
	counter := 0
	tIDs := make(chan string)
	for _, client := range clients {
		//fmt.Println("client is: ", client)
		go func(client *rpc.Client, tIDs chan string) {
			//fmt.Println("Client in loop: ", client)
			err = client.Call("KVServer.Commit", request, &response)
			CheckError("Commit Error", err, false)
			if err != nil || response.Success == false {
				transactionIsAborted = true
				err = transactionAlreadyCompletedError
				response.Success = false
				allSuccessful = false
			}
			tIDs <- strconv.Itoa(response.TxnID)
			counter++
		}(client, tIDs)
	}
	listOfIDs := make([]string, 0)
	for id := range tIDs {
		listOfIDs = append(listOfIDs, id)
		if len(listOfIDs) == len(clients) {
			close(tIDs)
		}
	}
	//fmt.Println("list of ids: ", listOfIDs)
	for counter < len(clients) {
		continue
	}
	stringID := getMostCommon(listOfIDs)
	intID, err := strconv.Atoi(stringID)
	if !allSuccessful {
		response.Success = false
		t.Abort()
	}
	return response.Success, intID, err
}

// Aborts the transaction.
func (t *mytx) Abort() {
	//fmt.Println("Abort")
	if transactionIsAborted {
		//fmt.Println("Transaction was already aborted")
		return
	}
	var response AbortResponse
	request := AbortRequest{TransactionId: t.TransactionID}
	for _, client := range clients {
		err := client.Call("KVServer.Abort", request, response)
		CheckError("Abort", err, false)
	}
	// set this transaction as dead forever
	transactionIsAborted = true
}

// Find the most common element in the list and return it
func getMostCommon(list []string) string {
	count := 1
	tempCount := 0
	temp := ""
	popular := list[0]
	for i := 0; i < (len(list) - 1); i++ {
		temp = list[i]
		tempCount = 0
		for j := 1; j < len(list); j++ {
			if temp == list[j] {
				tempCount++
			}
		}
		if tempCount > count {
			popular = temp
			count = tempCount
		}
	}
	return popular
}

func getAddress() string {
	ifaces, err := net.Interfaces()
	CheckError("", err, false)
	returnIP := ""
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		CheckError("", err, false)
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			//fmt.Println("Ip is: ", ip)
			if strings.Compare(ip.String(), "127.0.0.1") != 0 {
				returnIP = ip.String()
				return returnIP
			}
		}
	}
	return returnIP
}

// Simplified error checking function, returning true if there's no error, false otherwise
// If exit is true, also terminate the process
func CheckError(msg string, err error, exit bool) bool {
	if err != nil {

		////fmt.Println("ERROR: " + msg)
		if exit {
			panic(err)
			os.Exit(-1)
		} else {
			fmt.Fprintln(os.Stderr, msg, err)
		}
		return false
	}
	return true
}
