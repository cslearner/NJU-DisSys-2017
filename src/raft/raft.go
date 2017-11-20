package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import (
    //"fmt"
    "math/rand"
    "time"
)

// import "bytes"
// import "encoding/gob"

const (
    LEADER = iota
    CANDIDATE
    FOLLOWER

    HB_INTERVAL = 50 * time.Millisecond //50ms
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
    LogIndex int
    LogTerm int
    LogComd interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

    state int //raft peer state
    voteCount int
    //chanCommit chan bool
    chanHeartbeat chan bool
    chanGrantVote chan bool
    chanLeader chan bool
    chanApply chan ApplyMsg

    //persistent state on all server
    currentTerm int //current term number
    votedFor int //candidateID that received vote in currentTerm
    //log [] //ignored in lab2

    //volatile state on all server
    //commitIndex int
    //lastApplied int

    //volatile state on leader
    //nextIndex []int 
    //matchIndex []int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
    term = rf.currentTerm
    isleader = (rf.state == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
    Term int //candidate's term
    CandidateId int //candidate requesting vote 
    //LastLogIndex int
    //LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
    Term int //currentTerm
    VoteGranted bool //true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
    rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.VoteGranted = false
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        //reply.voteGranted = false
        return
    }

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.state = FOLLOWER
        rf.votedFor = -1
    }
    reply.Term = rf.currentTerm

    if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
        rf.chanGrantVote <- true
        rf.state = FOLLOWER
        rf.votedFor = args.CandidateId
        reply.VoteGranted = true
    }
}

type AppendEntriesArgs struct {
    Term int //leader's term
    LeaderId int
    //prevLogIndex int
    //prevLogTerm int 
    Entries []byte
    //leaderCommit int
}

type AppendEntriesReply struct {
    Term int //currentTerm
    //Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        return
    }

    rf.chanHeartbeat <- true

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.state = FOLLOWER
        rf.votedFor = -1
    }
    reply.Term = args.Term
}
//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if ok {
        //term := rf.currentTerm
        if rf.state != CANDIDATE {
            return ok
        }
        if args.Term != rf.currentTerm {
            return ok
        }
        if reply.Term > rf.currentTerm {
            rf.currentTerm = reply.Term
            rf.state = FOLLOWER
            rf.votedFor = -1
        }
        if reply.VoteGranted {
            rf.voteCount++
            if rf.state == CANDIDATE && rf.voteCount > len(rf.peers)/2 {
                rf.state = FOLLOWER
                rf.chanLeader <- true
            }
        }
    }
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if ok {
        if rf.state != LEADER {
            return ok
        }
        if args.Term != rf.currentTerm {
            return ok
        }
        if reply.Term > rf.currentTerm {
            rf.currentTerm = reply.Term
            rf.state = FOLLOWER
            rf.votedFor = -1
            return ok
        }
    }
    return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) broadcastRequestVote() {
    var args RequestVoteArgs
    rf.mu.Lock()
    args.Term = rf.currentTerm
    args.CandidateId = rf.me
    rf.mu.Unlock()

    for i := range rf.peers {
        if i != rf.me && rf.state == CANDIDATE {
            go func(i int) {
                var reply RequestVoteReply
                rf.sendRequestVote(i, args, &reply)
            }(i)
        }
    }
}

func (rf *Raft) broadcastAppendEntries() {
    var args AppendEntriesArgs
    rf.mu.Lock()
    args.Term = rf.currentTerm
    args.LeaderId = rf.me
    rf.mu.Unlock()

    for i := range rf.peers {
        if i != rf.me && rf.state == LEADER {
            go func(i int) {
                var reply AppendEntriesReply
                rf.sendAppendEntries(i, args, &reply)
            }(i)
        }
    }
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
    rf.state = FOLLOWER
    rf.currentTerm = 0
    rf.votedFor = -1
    rf.chanHeartbeat = make(chan bool, 10)
    rf.chanGrantVote = make(chan bool, 10)
    rf.chanLeader = make(chan bool, 10)
    rf.chanApply = applyCh

    //random election timeout
    //rand.Seed(time.Now().Unix())
    //timeout := rand.Int63n(300-150)+150

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    //state machine, begin as follower
    go func() {
        for {
            switch rf.state {
            //begin as FOLLOWER
            case FOLLOWER:
                select {
                case <-rf.chanHeartbeat:
                case <-rf.chanGrantVote:
                case <-time.After(time.Duration(rand.Int63()%150 + 150) * time.Millisecond):
                    rf.state = CANDIDATE
                }

            //send heartbeats every 50ms
            case LEADER:
                rf.broadcastAppendEntries()
                time.Sleep(HB_INTERVAL)

            case CANDIDATE:
                rf.mu.Lock()
                rf.currentTerm++
                rf.votedFor = rf.me
                rf.voteCount = 1
                //rf.persist()
                rf.mu.Unlock()

                //send RequestVote
                go rf.broadcastRequestVote()

                select {
                case <-time.After(time.Duration(rand.Int63()%150+150) * time.Millisecond):
                case <-rf.chanHeartbeat:
                    rf.state = FOLLOWER
                case <-rf.chanLeader:
                    rf.state = LEADER
                }
            }
        }
    }()


	return rf
}
