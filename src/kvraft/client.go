package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "sync/atomic"
import "log"

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int
	me       int64 
	msgId    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = 0
	ck.me = nrand()
	ck.msgId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	req := GetArgs{
		Key: key,
	}
	for i:=0; i<8000; i++{
		resp := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &req, &resp)
		if ok && !resp.WrongLeader {
			time.Sleep(time.Millisecond * 100)
			return resp.Value
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(time.Millisecond * 5)	
	}
	log.Fatalln("Get",key,"timeout")
	return ""
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	req := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Me:  ck.me,
		MsgId : atomic.AddInt64(&ck.msgId, 1) ,
	}
	for i:=0; i<8000; i++ {
		resp := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &req, &resp)
		if ok && !resp.WrongLeader {
			time.Sleep(time.Millisecond * 100)
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(time.Millisecond * 5)
	}
	log.Fatalln(op,key,value,"timeout")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
