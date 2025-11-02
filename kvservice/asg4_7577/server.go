package kvservice

import (
	"asg4/sysmonitor"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type requestInfo struct {
	RequestID     int64
	returnedValue string
}
type KVServer struct {
	l           net.Listener
	dead        bool // for testing
	unreliable  bool // for testing
	id          string
	monitorClnt *sysmonitor.Client
	view        sysmonitor.View
	done        sync.WaitGroup
	finish      chan interface{}

	// Add your declarations here.
	kvmap       map[string]string
	isPrimary   bool
	requests    map[string]requestInfo
	mu          sync.Mutex
	backupReady bool
}

func (server *KVServer) ForwardPut(args *PutArgs, reply *PutReply) error {
	server.mu.Lock()
	defer server.mu.Unlock()

	if server.id != server.view.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	request, exists := server.requests[args.ClientID]
	if exists && args.RequestID <= request.RequestID {
		reply.Err = OK
		return nil
	}

	// Directly apply the value sent from the primary
	server.kvmap[args.Key] = args.Value
	server.requests[args.ClientID] = requestInfo{
		RequestID: args.RequestID,
	}
	reply.Err = OK
	return nil
}

func (server *KVServer) ForwardGet(args *GetArgs, reply *GetReply) error {
	server.mu.Lock()
	defer server.mu.Unlock()

	if server.id != server.view.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	request, exists := server.requests[args.ClientID]
	if exists && args.RequestID <= request.RequestID {
		reply.Err = OK
		reply.Value = server.kvmap[args.Key]
		return nil
	}

	val, ok := server.kvmap[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return nil
	}

	reply.Err = OK
	reply.Value = val
	server.requests[args.ClientID] = requestInfo{
		RequestID: args.RequestID,
	}
	return nil
}

func (server *KVServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	DPrintf("\n[%s] Put called with key=%s, value=%s, client=%s, reqID=%d\n", server.id, args.Key, args.Value, args.ClientID, args.RequestID)

	var newValue string

	server.mu.Lock()

	// not primary return
	if !server.isPrimary {
		server.mu.Unlock()
		reply.Err = ErrWrongServer
		return nil
	}

	request, exists := server.requests[args.ClientID]
	// replicated request : reply ok & ignore
	if exists && args.RequestID <= request.RequestID {
		reply.Err = OK
		reply.PreviousValue = request.returnedValue
		//DPrintf("\n\nREPLICATED  preValue = %v\n\n", reply.PreviousValue)
		server.mu.Unlock()
		return nil
	}

	// New Request

	prevVal := server.kvmap[args.Key]
	// reply.PreviousValue = prevVal

	if args.DoHash {
		// putHash()
		newValue = strconv.Itoa(int(hash(prevVal + args.Value)))
		reply.PreviousValue = prevVal // reply previous value
	} else {
		// normal put()
		newValue = args.Value
		// reply.PreviousValue = ""
	}
	//DPrintf("\n\nPrevious value = %v\n\n", reply.PreviousValue)
	backup := server.view.Backup
	server.mu.Unlock()

	// Wait if backup exists but not yet ready
	if backup != "" && backup != server.id {
		for {
			server.mu.Lock()
			ready := server.backupReady
			server.mu.Unlock()
			if ready {
				break
			}
			time.Sleep(sysmonitor.PingInterval)
		}

		// Forward to backup
		forwardArgs := &PutArgs{
			Key:       args.Key,
			Value:     newValue,
			ClientID:  args.ClientID,
			RequestID: args.RequestID,
		}

		var backupReply PutReply
		ok := call(backup, "KVServer.ForwardPut", forwardArgs, &backupReply)

		if !ok || backupReply.Err != OK {
			DPrintf("[%s] WARNING: forward to backup %s failed (ok=%v, err=%v)\n", server.id, backup, ok, backupReply.Err)

		}
	}
	//Backup updated and ready to apply change in primary
	server.mu.Lock()
	server.kvmap[args.Key] = newValue

	server.requests[args.ClientID] = requestInfo{
		RequestID:     args.RequestID,
		returnedValue: reply.PreviousValue,
	}

	reply.Err = OK
	// DPrintf("[%s] Final write: key=%s, newValue='%s'\n", server.id, args.Key, newValue)
	server.mu.Unlock()

	return nil
}

func (server *KVServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	DPrintf("CLientID : %s RequestID : %d", args.ClientID, args.RequestID)
	var backup string
	server.mu.Lock()

	// not primary return
	if !server.isPrimary {
		reply.Err = ErrWrongServer
		server.mu.Unlock()
		return nil
	}

	request, exists := server.requests[args.ClientID]
	if exists && args.RequestID == request.RequestID {
		// Return cached result for duplicate request
		val := server.kvmap[args.Key]
		reply.Err = OK
		reply.Value = val
		// DPrintf("[%s] Get (duplicate): key=%s value=%s\n", server.id, args.Key, val)
		server.mu.Unlock()
		return nil
	}

	// new request
	val, ok := server.kvmap[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		server.mu.Unlock()
		return nil
	}

	reply.Err = OK
	reply.Value = val
	// DPrintf("[%s] Get: key=%s value=%s\n", server.id, args.Key, val)

	backup = server.view.Backup

	server.mu.Unlock()

	// Forward to backup and wait for confirmation
	if backup != "" && backup != server.id {
		var backupReply GetReply

		ok := call(backup, "KVServer.ForwardGet", args, &backupReply)

		if !ok || backupReply.Err == ErrWrongServer {
			reply.Err = ErrWrongServer
			return nil
		}

		if backupReply.Err != OK && backupReply.Err != ErrNoKey {
			reply.Err = backupReply.Err
			return nil
		}
	}

	// Record this request ID after everything
	server.mu.Lock()
	server.requests[args.ClientID] = requestInfo{
		RequestID: args.RequestID,
	}
	server.mu.Unlock()

	return nil
}

func (server *KVServer) SyncState(args *SyncStateArgs, reply *SyncStateReply) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.isPrimary {
		reply.Err = ErrWrongServer
		// DPrintf("Primary in syncState\n")
		return nil
	}
	// DPrintf("Hello in SyncState\n")
	server.kvmap = args.KVMap
	server.requests = args.Requests
	server.backupReady = true
	reply.Err = OK
	return nil
}

// functions to copy all the Kvmap,requests needed to sync state
func copyMap(original map[string]string) map[string]string {
	newMap := make(map[string]string)
	for k, v := range original {
		newMap[k] = v
	}
	return newMap
}

func copyRequests(original map[string]requestInfo) map[string]requestInfo {
	newMap := make(map[string]requestInfo)
	for k, v := range original {
		newMap[k] = v
	}
	return newMap
}

// ping the view server periodically.
func (server *KVServer) tick() {

	// This line will give an error initially as view and err are not used.
	view, err := server.monitorClnt.Ping(server.view.Viewnum)

	// Your code here.
	if err != nil {
		return
	}

	if view != server.view {
		// DPrintf("[%s] New view: primary=%s, backup=%s\n", server.id, view.Primary, view.Backup)
	}
	oldView := server.view
	server.view = view

	// not primary
	if server.id != view.Primary {
		server.isPrimary = false
		return
	}

	server.isPrimary = true

	// reset backup after promoting
	if oldView.Primary != view.Primary && server.id == view.Primary {
		// We just became the primary
		server.backupReady = false
		// DPrintf("[%s] Became primary in new view, resetting backupReady\n", server.id)
	}
	if view.Backup != oldView.Backup {
		server.backupReady = false
	}

	// If there's a backup and we are not synced with it yet
	if view.Backup != "" && !server.backupReady {
		args := &SyncStateArgs{
			KVMap:    copyMap(server.kvmap),
			Requests: copyRequests(server.requests),
		}

		for {
			// Stop retrying if backup changes during retries
			if server.view.Backup != view.Backup || server.view.Primary != server.id {
				break
			}

			var reply SyncStateReply
			ok := call(view.Backup, "KVServer.SyncState", args, &reply)

			if ok && reply.Err == OK {
				server.backupReady = true
				break
			}

			// Try updating view before retrying
			updatedView, err := server.monitorClnt.Ping(server.view.Viewnum)
			if err != nil {
				return
			}
			server.view = updatedView
		}
	}

}

// tell the server to shut itself down.
// please do not change this function.
func (server *KVServer) Kill() {
	server.dead = true
	server.l.Close()
}

func StartKVServer(monitorServer string, id string) *KVServer {
	server := new(KVServer)
	server.id = id
	server.monitorClnt = sysmonitor.MakeClient(id, monitorServer)
	server.view = sysmonitor.View{}
	server.finish = make(chan interface{})

	// Add your server initializations here
	// ==================================
	server.kvmap = make(map[string]string)
	server.isPrimary = false
	server.requests = make(map[string]requestInfo)
	//====================================

	rpcs := rpc.NewServer()
	rpcs.Register(server)

	os.Remove(server.id)
	l, e := net.Listen("unix", server.id)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	server.l = l

	// please do not make any changes in the following code,
	// or do anything to subvert it.

	go func() {
		for server.dead == false {
			conn, err := server.l.Accept()
			if err == nil && server.dead == false {
				if server.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if server.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					server.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						server.done.Done()
					}()
				} else {
					server.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						server.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && server.dead == false {
				fmt.Printf("KVServer(%v) accept: %v\n", id, err.Error())
				server.Kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", server.id)
		server.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(server.finish)
	}()

	server.done.Add(1)
	go func() {
		for server.dead == false {
			server.tick()
			time.Sleep(sysmonitor.PingInterval)
		}
		server.done.Done()
	}()

	return server
}
