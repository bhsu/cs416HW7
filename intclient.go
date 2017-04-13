/*

A client that evalutes commands interactively, using the kvservice implemented in assignment 6 of
UBC CS 416 2016W2. More specifically, it does the following:

	1. Read input from the user.
	2. Evaluate the command by calling the kvservice code
	3. Print the result
	4. Loop back to step 1

For this client to work properly, kvservice.go must exist in ./kvservice/, relative to this file.

Usage: go run client.go

*/

package main

import (
	"./kvservice"
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"strconv"
)

// List of nodes' listening addresses for client connections
var NODES = []string{
	"198.162.33.46:9988",
  //"98.76.54.32:10"
}

// printUsage prints the usage information for the client.
func printUsage() {
	fmt.Printf("\033[1m================= Simple Interactive Client =================\033[0m\n" +
		"Client commands:\n" +
		"  'n' to create a new connection\n" +
		"  'q' to quit\n" +
		"Conn commands:\n" +
		"  't' to create a new transaction\n" +
		"  'gc <Ip:Port> <prevHash>' to get list of children with parent hash prevHash" +
		"  'x' to close the connection\n" +
		"  'q' to quit\n" +
		"Txn commands:\n" +
		"  'p <key> <value>' to put a value into the key-value store\n" +
		"  'g <key>' to retrieve a value from the key-value store\n" +
		"  'c' to commit a transaction\n" +
		"  'a' to abort a transaction\n" +
		"  'q' to quit\n" +
		"\033[1m-------------------------------------------------------------\033[0m\n")
}

// the main function.
func main() {
	printUsage()
	var scanner *bufio.Scanner = bufio.NewScanner(os.Stdin)
	for {
		input, err := promptInput(scanner, "Client")
		exitOnError(err)
		switch input[0] {
		case "n":
			conn := kvservice.NewConnection(NODES)
			fmt.Printf("New connection created: %v.\n", conn)
		CONN_LOOP:
			for {
				input, err = promptInput(scanner, "Conn")
				exitOnError(err)
				switch input[0] {
				case "t":
					tx, err := conn.NewTX()
					exitOnError(err)
					fmt.Printf("New transaction created: %v.\n", tx)
				TXN_LOOP:
					for {
						input, err = promptInput(scanner, "Txn")
						exitOnError(err)
						switch input[0] {
						case "p":
							succ, err := tx.Put(kvservice.Key(input[1]), kvservice.Value(input[2]))
							fmt.Printf("Success: %v. Error: %v.\n", succ, err)
							if err != nil {
								break TXN_LOOP
							}
						case "g":
							success, value, err := tx.Get(kvservice.Key(input[1]))
							fmt.Printf("Success: %v. Value: %v. Error: %v.\n", success, value, err)
							if err != nil {
								break TXN_LOOP
							}
						case "c":
							val,err := strconv.Atoi(input[1])
							if err != nil {
								fmt.Println("some error", err)
							}
							success, txId, err := tx.Commit(val)
							fmt.Printf("Success: %v. Txn ID: %v. Error: %v.\n", success, txId, err)
							break TXN_LOOP
						case "a":
							tx.Abort()
							fmt.Printf("Transaction aborted.\n")
							break TXN_LOOP
						case "q":
							os.Exit(0)
						}
					}
				case "x":
					conn.Close()
					fmt.Printf("Connection closed.\n")
					break CONN_LOOP
				case "gc":
					fmt.Print("input[2]: ", input[2])
					children := conn.GetChildren(input[1], input[2])
					fmt.Println("Children are: ", children)
					break CONN_LOOP
				case "q":
					os.Exit(0)
				}
			}
		case "q":
			os.Exit(0)
		}
	}
	exitOnError(scanner.Err())
}

// promptInput prompts for input from the console, and returns an array of words from the user
func promptInput(scanner *bufio.Scanner, prefix string) (input []string, err error) {
	fmt.Print("\033[1m" + prefix + ">\033[0m ")
	if !scanner.Scan() {
		return nil, fmt.Errorf("Unable to read input")
	}
	input = strings.Split(scanner.Text(), " ")
	if len(input) == 0 {
		return promptInput(scanner, prefix)
	}
	return input, nil
}

// exitOnError terminates the current process if err is not nil, logging the error in the process.
func exitOnError(err error) {
	if err != nil {
		log.Fatalln("[ERROR]", err)
	}
}