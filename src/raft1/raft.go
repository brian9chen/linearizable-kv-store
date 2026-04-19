package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int
	dead      int32

	// Persistent state (Figure 2)
	currentTerm int
	votedFor    int
	// log entries; the entry at offset 0 is a sentinel holding the
	// term of the snapshot's last included index (or term 0 for no
	// snapshot). With a snapshot whose lastIncludedIndex = S, the
	// absolute index of log[i] is S + i.
	log               []LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int

	// Volatile state
	commitIndex int
	lastApplied int

	// Leader state (reinitialized after election)
	nextIndex  []int
	matchIndex []int

	role          Role
	lastHeardAt   time.Time
	electionReset time.Duration

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond

	// pending snapshot that needs to be delivered on applyCh
	pendingSnapshot      []byte
	pendingSnapshotIndex int
	pendingSnapshotTerm  int
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// expects rf.mu held.
func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

// expects rf.mu held.
func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// expects rf.mu held; index must be >= lastIncludedIndex.
func (rf *Raft) termAt(index int) int {
	return rf.log[index-rf.lastIncludedIndex].Term
}

// expects rf.mu held.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// expects rf.mu held; saves both raft state and snapshot atomically.
func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}
	if index > rf.lastLogIndex() {
		return
	}

	newLastTerm := rf.termAt(index)
	// Build new log: index at offset 0 is the snapshot sentinel.
	newLog := make([]LogEntry, 1, len(rf.log)-(index-rf.lastIncludedIndex))
	newLog[0] = LogEntry{Term: newLastTerm}
	newLog = append(newLog, rf.log[index-rf.lastIncludedIndex+1:]...)
	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = newLastTerm

	rf.persistWithSnapshot(snapshot)
}

// =============== RequestVote RPC ===============

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	// Candidate log must be at least as up-to-date.
	myLastIdx := rf.lastLogIndex()
	myLastTerm := rf.lastLogTerm()
	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIdx)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeardAt = time.Now()
		rf.persist()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// =============== AppendEntries RPC ===============

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// Fast backup hints (for speeding up nextIndex decrement).
	ConflictTerm  int
	ConflictIndex int
	XLen          int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1
	reply.XLen = rf.lastLogIndex() + 1

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	// Valid AppendEntries from current leader: reset election timer.
	rf.role = Follower
	rf.lastHeardAt = time.Now()

	// Reject if prev log entry is before our snapshot (already applied).
	if args.PrevLogIndex < rf.lastIncludedIndex {
		// We can still try to accept entries that come after our snapshot.
		// Compute offset into args.Entries that starts at lastIncludedIndex+1.
		startAt := rf.lastIncludedIndex - args.PrevLogIndex
		if startAt > len(args.Entries) {
			// All entries are already in our snapshot.
			reply.Success = true
			// Update commit index conservatively.
			if args.LeaderCommit > rf.commitIndex {
				newCommit := args.LeaderCommit
				if newCommit > rf.lastLogIndex() {
					newCommit = rf.lastLogIndex()
				}
				if newCommit > rf.commitIndex {
					rf.commitIndex = newCommit
					rf.applyCond.Signal()
				}
			}
			return
		}
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = rf.lastIncludedTerm
		args.Entries = args.Entries[startAt:]
	}

	// Reject if we don't have args.PrevLogIndex.
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictIndex = rf.lastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}

	// Check term match at PrevLogIndex.
	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		reply.ConflictTerm = rf.termAt(args.PrevLogIndex)
		// Find first index of ConflictTerm.
		idx := args.PrevLogIndex
		for idx > rf.lastIncludedIndex && rf.termAt(idx-1) == reply.ConflictTerm {
			idx--
		}
		reply.ConflictIndex = idx
		return
	}

	// Merge entries. Only overwrite where terms conflict (to avoid
	// truncating entries from later, re-ordered RPCs).
	for i, e := range args.Entries {
		absIdx := args.PrevLogIndex + 1 + i
		if absIdx > rf.lastLogIndex() {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
		if rf.termAt(absIdx) != e.Term {
			rf.log = rf.log[:absIdx-rf.lastIncludedIndex]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		last := rf.lastLogIndex()
		if newCommit > last {
			newCommit = last
		}
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Signal()
		}
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// =============== InstallSnapshot RPC ===============

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	}
	rf.role = Follower
	rf.lastHeardAt = time.Now()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// Build new log. If we have the entry at LastIncludedIndex with
	// matching term, keep entries after it; otherwise discard all.
	if args.LastIncludedIndex <= rf.lastLogIndex() &&
		rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		newLog := make([]LogEntry, 1)
		newLog[0] = LogEntry{Term: args.LastIncludedTerm}
		newLog = append(newLog, rf.log[args.LastIncludedIndex-rf.lastIncludedIndex+1:]...)
		rf.log = newLog
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.persistWithSnapshot(args.Data)

	// Queue snapshot for delivery on applyCh.
	rf.pendingSnapshot = args.Data
	rf.pendingSnapshotIndex = args.LastIncludedIndex
	rf.pendingSnapshotTerm = args.LastIncludedTerm
	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

// =============== State transitions (rf.mu held) ===============

func (rf *Raft) becomeFollower(term int) {
	rf.role = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	last := rf.lastLogIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = last + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = last
	// Immediately send heartbeats.
	go rf.broadcastAppendEntries()
}

// =============== Start ===============

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}
	entry := LogEntry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)
	index := rf.lastLogIndex()
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.persist()
	go rf.broadcastAppendEntries()
	return index, rf.currentTerm, true
}

// =============== Election ticker ===============

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.lastHeardAt) >= rf.electionReset {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// expects rf.mu held.
func (rf *Raft) resetElectionTimer() {
	rf.lastHeardAt = time.Now()
	// 400-700ms: tester allows 1 second for elections.
	rf.electionReset = time.Duration(400+rand.Intn(300)) * time.Millisecond
}

// expects rf.mu held.
func (rf *Raft) startElection() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	rf.persist()

	term := rf.currentTerm
	lastIdx := rf.lastLogIndex()
	lastTerm := rf.lastLogTerm()

	votes := int32(1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(peer, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm != term || rf.role != Candidate {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if reply.VoteGranted {
				n := atomic.AddInt32(&votes, 1)
				if int(n)*2 > len(rf.peers) && rf.role == Candidate {
					rf.becomeLeader()
				}
			}
		}(i)
	}
}

// =============== Heartbeat / replication loop ===============

func (rf *Raft) heartbeatLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := rf.role == Leader
		rf.mu.Unlock()
		if isLeader {
			rf.broadcastAppendEntries()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateTo(i)
	}
}

func (rf *Raft) replicateTo(peer int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	next := rf.nextIndex[peer]

	// If follower needs entries older than our snapshot, send InstallSnapshot.
	if next <= rf.lastIncludedIndex {
		args := &InstallSnapshotArgs{
			Term:              term,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()

		reply := &InstallSnapshotReply{}
		if !rf.sendInstallSnapshot(peer, args, reply) {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm != term || rf.role != Leader {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
		}
		if args.LastIncludedIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = args.LastIncludedIndex
		}
		return
	}

	prevIdx := next - 1
	prevTerm := rf.termAt(prevIdx)
	entries := make([]LogEntry, len(rf.log)-(next-rf.lastIncludedIndex))
	copy(entries, rf.log[next-rf.lastIncludedIndex:])
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	if !rf.sendAppendEntries(peer, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != term || rf.role != Leader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatch
		}
		if newMatch+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = newMatch + 1
		}
		rf.advanceCommit()
	} else {
		// Fast backup.
		if reply.ConflictTerm == -1 {
			if reply.ConflictIndex > 0 {
				rf.nextIndex[peer] = reply.ConflictIndex
			} else if reply.XLen > 0 {
				rf.nextIndex[peer] = reply.XLen
			}
		} else {
			// Search for last entry with ConflictTerm in our log.
			found := -1
			for i := rf.lastLogIndex(); i > rf.lastIncludedIndex; i-- {
				if rf.termAt(i) == reply.ConflictTerm {
					found = i
					break
				}
			}
			if found != -1 {
				rf.nextIndex[peer] = found + 1
			} else {
				rf.nextIndex[peer] = reply.ConflictIndex
			}
		}
		if rf.nextIndex[peer] < 1 {
			rf.nextIndex[peer] = 1
		}
		// Retry soon.
		go rf.replicateTo(peer)
	}
}

// expects rf.mu held.
func (rf *Raft) advanceCommit() {
	// Find highest N such that a majority of matchIndex[i] >= N
	// and log[N].Term == currentTerm.
	for N := rf.lastLogIndex(); N > rf.commitIndex; N-- {
		if N <= rf.lastIncludedIndex {
			break
		}
		if rf.termAt(N) != rf.currentTerm {
			continue
		}
		count := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= N {
				count++
			}
		}
		if count*2 > len(rf.peers) {
			rf.commitIndex = N
			rf.applyCond.Signal()
			break
		}
	}
}

// =============== Apply goroutine ===============

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.pendingSnapshot != nil {
			data := rf.pendingSnapshot
			idx := rf.pendingSnapshotIndex
			term := rf.pendingSnapshotTerm
			rf.pendingSnapshot = nil
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      data,
				SnapshotIndex: idx,
				SnapshotTerm:  term,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			if idx > rf.lastApplied {
				rf.lastApplied = idx
			}
			continue
		}
		if rf.lastApplied < rf.commitIndex && rf.lastApplied >= rf.lastIncludedIndex {
			next := rf.lastApplied + 1
			if next <= rf.lastIncludedIndex {
				rf.lastApplied = rf.lastIncludedIndex
				continue
			}
			entry := rf.log[next-rf.lastIncludedIndex]
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: next,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			if next > rf.lastApplied {
				rf.lastApplied = next
			}
			continue
		}
		rf.applyCond.Wait()
	}
}

// =============== Kill ===============

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// =============== Make ===============

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = []LogEntry{{Term: 0}}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.readPersist(persister.ReadRaftState())

	rf.mu.Lock()
	rf.resetElectionTimer()
	// If we restarted with a snapshot, queue it for delivery.
	if rf.lastIncludedIndex > 0 {
		snap := persister.ReadSnapshot()
		if snap != nil && len(snap) > 0 {
			rf.pendingSnapshot = snap
			rf.pendingSnapshotIndex = rf.lastIncludedIndex
			rf.pendingSnapshotTerm = rf.lastIncludedTerm
		}
	}
	rf.mu.Unlock()

	go rf.ticker()
	go rf.heartbeatLoop()
	go rf.applier()

	return rf
}
