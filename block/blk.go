/*
This class contains blocks operation logic and methods

[ghash] : SHA 256 hash in hexadecimal of the genesis block for this instantiation of the system (new in A7).
[num-zeroes] : required number of leading zeroes in the proof-of-work algorithm, greater or equal to 1 (new in A7).
*/
package blk

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	//"sync"
	"time"
)

var (
	IsBuildlingTransactionBlock bool // signals if a transaction block needs to be build
	ReceiveBlock             bool // Flag to check if node has received a txn block from another node
	BlockIsValid                = 0  // 0 is default, 1: block is invalid, 2: block is valid
	BroadcastedBlock            Block
	//SetBlock = false
	//broadcastBlockLock sync.RWMutex

)

type Block struct {
	PrevHash      []byte
	TransactionID int
	NodeID        int
	Nonce         uint32
	ChildBlocks   []*Block
	BlockType     int
	ParentBlock   *Block
	SelfHash      []byte
	IsValidated   bool
}

// proof of work hashing for transaction block
// block type 0 for noop block and 1 for transaction block
func Hashing(ghs []byte, nid int, tid int, blockType int, numzeroes int) ([]byte, uint32) {
	debugLog("In hashing")
	// convert all args to string
	digest := ""
	hashoutput := []byte{}
	hasherOutput := []string{}
	nodeId := strconv.Itoa(nid)
	transactionId := strconv.Itoa(tid)
	var nonce uint32 = 0
	ghash := string(ghs[:])
	noncestr := ""
	argsArray := []string{}

	leadingZeroNotMatched := true
	for leadingZeroNotMatched {
		// Check if the node has received a block from another node that finished already
		if ReceiveBlock {
			//debugLog("In receiveblock")
			for true {
				if BroadcastedBlock.TransactionID != tid {
					break
				}
				if BlockIsValid == 1 {
					debugLog("block is invalid here")
					// Block is invalid, so continue generating this block
					ReceiveBlock = false
					BlockIsValid = 0
					break
				} else if BlockIsValid == 2 {
					//if BroadcastedBlock.TransactionID != tid {
					//	break
					//}
					debugLog("block is  valid here")
					// Block is valid, so stop generating this block and append the new block
					BlockIsValid = 0
					ReceiveBlock = false
					return nil, 0
				}
			}
		}
		nonce++ // nonce get increment by 1 for every iteration
		// uint32 converts to uint64 converts to string
		j := uint64(nonce)
		noncestr = strconv.FormatUint(uint64(j), 10)

		// blockType for transaction block
		if blockType == 1 {
			argsArray = []string{nodeId, transactionId, noncestr, ghash}
		} else {
			argsArray = []string{nodeId, noncestr, ghash}
		}
		stringToHash := strings.Join(argsArray, "")
		agrsToHash := []byte(stringToHash)
		// hashing
		hasher := sha256.New()
		hasher.Write(agrsToHash)
		digest = (hex.EncodeToString(hasher.Sum(nil)))
		//fmt.Println("digest:", digest, "counts ", nonce)
		hasherOutput = strings.Split(digest, "")

		// check for non leading zero digest
		if numzeroes == 0 {
			//fmt.Println("correct hash", digest)
			hashoutput = []byte(digest)
			return hashoutput, nonce
		}

		// checking for leading zeros
		for _, i := range hasherOutput[:numzeroes] {
			if i != "0" {
				leadingZeroNotMatched = true
				break
			}
			leadingZeroNotMatched = false
		}

	}
	fmt.Println("correct hash", digest, "correct nonce", nonce)
	hashoutput = []byte(digest)
	return hashoutput, nonce
}

func ValidateHash(ghs []byte, nid int, tid int, blockType int, numzeroes int, nonce uint32) []byte {
	//debugLog("In ValidateHash")
	digest := ""
	nodeId := strconv.Itoa(nid)
	transactionId := strconv.Itoa(tid)
	ghash := string(ghs[:])
	noncestr := ""
	argsArray := []string{}
	j := uint64(nonce)
	noncestr = strconv.FormatUint(uint64(j), 10)

	// blockType for transaction block
	if blockType == 1 {
		argsArray = []string{nodeId, transactionId, noncestr, ghash}
	} else {
		argsArray = []string{nodeId, noncestr, ghash}
	}
	stringToHash := strings.Join(argsArray, "")
	agrsToHash := []byte(stringToHash)
	// hashing
	hasher := sha256.New()
	hasher.Write(agrsToHash)
	digest = (hex.EncodeToString(hasher.Sum(nil)))
	//fmt.Println("digest:", digest, "counts ", nonce)
	hashoutput := []byte(digest)
	return hashoutput
}

// mining of no-op blocks, this method runs in the background until MineTransactionBlock() is called.
// Resumes after MineTransactionBlock() is finished
func MineNoopBlock(nodeId int, blockchain *Block, numZeroes int) *Block {
	//debugLog("In MineNoopBlock...")
	if !IsBuildlingTransactionBlock {
		//debugLog("Im not building a txn block")
		// use the values from pre block in the chain
		lastBlock := GetLastBlockOfLongestBlockChain(blockchain)

		prehash := lastBlock.SelfHash
		// transaction id is 0 for noop block
		tid := 0
		nid := nodeId
		selfHash, nonce := Hashing(prehash, nid, tid, 0, numZeroes)
		var noopBlock Block
		if selfHash == nil && nonce == 0 {
			debugLog("selfhash is nil and nonce is 0")
			noopBlock = BroadcastedBlock
			noopBlock.ParentBlock = lastBlock
		} else {
			debugLog("making its own block")
			noopBlock = Block{
				PrevHash:    prehash,
				NodeID:      nid,
				Nonce:       nonce,
				ChildBlocks: nil,
				ParentBlock: lastBlock,
				BlockType:   0,
				SelfHash:    selfHash,
				IsValidated: false,
			}
		}
		return &noopBlock
	} else {
		// do nothing
	}

	return nil
}

// mining of transaction blocks
// this method should be called when client commits a transaction
func MineTransactionBlock(tid int, nodeId int, chain *Block, numZeroes int) *Block {
	debugLog("In MineTransactionBlock")
	// use the values from pre block in the chain
	lastBlock := GetLastBlockOfLongestBlockChain(chain)
	prehash := lastBlock.SelfHash
	nid := nodeId
	selfHash, nonce := Hashing(prehash, nid, tid, 1, numZeroes)
	var txBlock Block
	if selfHash == nil && nonce == 0 {
		txBlock = BroadcastedBlock
		txBlock.ParentBlock = lastBlock
	} else {
		debugLog("Creating new block")
		txBlock = Block{
			PrevHash:      prehash,
			NodeID:        nid,
			Nonce:         nonce,
			ChildBlocks:   nil,
			TransactionID: tid,
			ParentBlock:   lastBlock,
			BlockType:     1,
			SelfHash:      selfHash,
			IsValidated:   false,
		}
	}
	// add the new block as the last block's child
	//lastBlock.ChildBlocks = append(lastBlock.ChildBlocks, &txBlock)
	return &txBlock
}

// validate blocks passed in by other nodes
//  a block computed by a node is always guranteed to be unique, thus we need check the node passed in by other node is valid.
func ValidateBlock(mainBlock Block, numzeros int) bool {
	// use the info from mainBlock to hash it again and compare if the output hash is the same.
	debugLog("In ValidateBlock")
	// validating hash blocks
	ReceiveBlock = true

	outputhash := ValidateHash(mainBlock.PrevHash, mainBlock.NodeID, mainBlock.TransactionID,
		mainBlock.BlockType, numzeros, mainBlock.Nonce)
	if bytes.Compare(outputhash, mainBlock.SelfHash) == 0 {
		// hash matches
		debugLog("Hashes match")
		BroadcastedBlock = mainBlock
		BlockIsValid = 2
		return true
	} else {
		// hash does not match
		debugLog("hashes do not match")
		BlockIsValid = 1
		return false
	}

}

// Returns the last block in the longest chain
func GetLastBlockOfLongestBlockChain(block *Block) *Block {
	//debugLog("In GetLastBlockOfLongestBlockChain ")
	chain := block
	//debugLog("in getlastblock", block)
	for len(chain.ChildBlocks) != 0 {
		if len(chain.ChildBlocks) > 1 {
			// deal with fork
			debugLog("Trying to deal with fork")
			chain = FindLongestChildPath(chain.ChildBlocks)
		} else {
			//debugLog("Only one child here")
			chain = chain.ChildBlocks[0]
			//debugLog("Child has this many block: ", len(chain.ChildBlocks))
		}
	}
	//debugLog("Returning last block: ", chain)
	return chain
}

// Returns the block that leads to the longest path at a fork in the block chain
func FindLongestChildPath(chain []*Block) *Block {
	//debugLog("In FindLongestChildPath")
	var longestChild, tmpBlock *Block
	length := 0
	longestChain := 0

	// Iterate through all blocks at this fork
	for _, child := range chain {
		length = 0
		tmpBlock = child
		for len(tmpBlock.ChildBlocks) != 0 {
			if len(tmpBlock.ChildBlocks) > 1 {
				// Another fork in the chain so find longest path
				tmpBlock = FindLongestChildPath(child.ChildBlocks)
			} else {
				tmpBlock = tmpBlock.ChildBlocks[0]
			}
			length++
		}
		if longestChild == nil {
			longestChild = child
			longestChain = length
		} else {
			// If the current path is the longest, set the current path
			if length > longestChain || longestChain == 0 {
				longestChild = child
				longestChain = length
			} else {
				// This path is shorter than a previous path, so do nothing
				continue
			}
		}

	}
	return longestChild
}

// logging that can be disabled
func debugLog(v ...interface{}) {
	fmt.Println(v...)
}

// Returns the string representation of a byte array (or hash)
func toString(b []byte) string {
	s := bytes.NewBuffer(b).String()
	return s
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

// Search for the block in the chain with TxnID == tid
func PrintChain(block *Block) {
	for len(block.ChildBlocks) != 0 {
		//debugLog("Block: ", block.TransactionID, block.ChildBlocks, block.BlockType, BlockChain.IsValidated, block.Nonce)
		printBlock(block)
		time.Sleep(1 * time.Second)
		block = block.ChildBlocks[0]
	}
	printBlock(block)
}

func printBlock(block *Block) {
	fmt.Println("TransactionID:", block.TransactionID, "NodeID:", block.NodeID, "Nonce:", block.Nonce, "BlockType:", block.BlockType, "IsValidated:", block.IsValidated, "ChildBlocks:", block.ChildBlocks, "\n")
}
