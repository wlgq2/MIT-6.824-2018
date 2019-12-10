package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Me    int64
	MsgId int64
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	Me    int64
	MsgId int64
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Me    int64
	MsgId int64
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func MergeGroups(config *Config,groups map[int][]string) {
	for key,value := range groups {
		servers,ok := config.Groups[key]
		if ok {
			servers = append(servers,value...)
		} else {
			//获取分片数量最大组，写入新组。
			maxGroup := GetMaxCountShards(&(config.Shards))
			config.Shards[maxGroup[0]] = key
			servers = value
		}
		config.Groups[key] = servers
	}
}

func DeleteGroups(config *Config,groups []int) {
	for i:=0;i<len(groups);i++ {
		group := groups[i]
		_,ok := config.Groups[group]
		if ok {
			//获取该组分片
			shards := GetCountGroup(&(config.Shards),group)
			//遍历依次加入最小组
			for j:=0;j<len(shards);j++ {
				min := GetMinCountShards(&(config.Shards),group)
				config.Shards[shards[j]] = min
			}
			//删除组
			delete(config.Groups,group)
		}
	}
}

func DistributionGroups(config *Config) {
	index := 0
	for key,_ := range config.Groups {
		config.Shards[index%NShards] = key
		index++
	}
}

func GetCountGroup(Shards *[NShards]int,group int) (rst []int) {
	for i:=0; i<len(*Shards);i++ {
		if (*Shards)[i] == group {
			rst = append(rst, i)
		}
	}
	return 
}

func GetCountShards(Shards *[NShards]int) (rst map[int][]int) {
	for i:=0; i<len(*Shards); i++ {
		group := (*Shards)[i]
		value,ok := rst[group]
		if ok {
			value = append(value,i)
		} else {
			value = make([]int,1)
			value[0] = i
		}
 		rst[group] = value
	}
	return
}

func GetMaxCountShards(Shards *[NShards]int) (rst []int) {
	maps := GetCountShards(Shards)
	max := 0
	for _,value := range maps {
		if len(value) > max {
			rst = value
			max = len(value)
		}
	}
	return
}

func GetMinCountShards(Shards *[NShards]int, without int) (rst int) {
	maps := GetCountShards(Shards)
	min := NShards+1
	index := 0
	for _,value := range maps {
		if len(value) < min {
			index = value[0]
			min = len(value)
		}
	}
	rst = (*Shards)[index]
	return
}
