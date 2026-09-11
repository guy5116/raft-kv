// Command raftkv runs an interactive 3-node cluster in one process — a
// quick way to poke at your implementation once milestone 2 passes:
//
//	go run ./cmd/raftkv
//	> put color orange
//	> get color
//	orange
//	> crash 0        (kill a node — the cluster keeps serving)
//	> restart 0
//	> leader         (who is leader right now?)
//
// Stretch goal, great for the README demo GIF: replace the in-process
// harness with real processes talking over a TCP transport (implement
// transport.Transport with net/rpc or gRPC — the Raft code won't change
// at all, which is the whole point of the interface).
package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/guy5116/raft-kv/harness"
)

func main() {
	fmt.Println("starting 3-node raft-kv cluster...")
	c := harness.NewKVCluster(3, time.Now().UnixNano(), -1)
	defer c.Shutdown()

	if leader, term, err := c.CheckSingleLeader(); err == nil {
		fmt.Printf("leader: node %d (term %d)\n", leader, term)
	} else {
		fmt.Println("no leader yet — is milestone 1 implemented?")
	}
	clerk := c.Clerk()

	sc := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 {
			fmt.Print("> ")
			continue
		}
		switch fields[0] {
		case "put":
			if len(fields) == 3 {
				clerk.Put(fields[1], fields[2])
				fmt.Println("ok")
			} else {
				fmt.Println("usage: put <key> <value>")
			}
		case "append":
			if len(fields) == 3 {
				clerk.Append(fields[1], fields[2])
				fmt.Println("ok")
			} else {
				fmt.Println("usage: append <key> <value>")
			}
		case "get":
			if len(fields) == 2 {
				fmt.Println(clerk.Get(fields[1]))
			} else {
				fmt.Println("usage: get <key>")
			}
		case "crash", "restart", "disconnect", "reconnect":
			if len(fields) != 2 {
				fmt.Printf("usage: %s <node-id>\n", fields[0])
				break
			}
			id, err := strconv.Atoi(fields[1])
			if err != nil || id < 0 || id >= c.N() {
				fmt.Println("bad node id")
				break
			}
			switch fields[0] {
			case "crash":
				c.Crash(id)
			case "restart":
				c.Restart(id)
				clerk = c.Clerk() // pick up the restarted server
			case "disconnect":
				c.Disconnect(id)
			case "reconnect":
				c.Reconnect(id)
			}
			fmt.Println("ok")
		case "leader":
			if leader, term, err := c.CheckSingleLeader(); err == nil {
				fmt.Printf("node %d (term %d)\n", leader, term)
			} else {
				fmt.Println(err)
			}
		case "quit", "exit":
			return
		default:
			fmt.Println("commands: put, append, get, crash, restart, disconnect, reconnect, leader, quit")
		}
		fmt.Print("> ")
	}
}
