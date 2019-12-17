package shardmaster

import "strconv"
import "sort"

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
	Me      int64
	MsgId   int64
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	Me    int64
	MsgId int64
	GIDs  []int
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

//复制组
func CopyGroups(config *Config, groups map[int][]string) {
	config.Groups = make(map[int][]string)
	for key,value := range groups {
	  config.Groups[key] = value
	}
}

//合并组和分片
func MergeGroups(config *Config, groups map[int][]string) {
	for key, value := range groups {
		servers, ok := config.Groups[key]
		if ok {
			servers = append(servers, value...)
		} else {
			for cnt := 0; ;cnt++{
				//遍历获取分片数量最大组，写入新组。
				maxGroup,maxGroups := GetMaxCountShards(config)
				if cnt >= len(maxGroups)-1 && maxGroup != 0{
					//分配均匀，完成分配
					break
				}
				config.Shards[maxGroups[0]] = key	
			}
			servers = value
		}
		config.Groups[key] = servers
	}
}

//删除组及分片
func DeleteGroups(config *Config, groups []int) {
	for i := 0; i < len(groups); i++ {
		group := groups[i]
		_, ok := config.Groups[group]
		if ok {
			//获取该组分片
			shards := GetCountGroup(&(config.Shards), group)
			//遍历依次加入最小组
			for j := 0; j < len(shards); j++ {
				min := GetMinCountShards(config, group)
				config.Shards[shards[j]] = min
			}
			//删除组
			delete(config.Groups, group)
		}
	}
}

//排序
func SortGroup(groups map[int][]string)  []int {
    var keys []int
    for key,_ := range groups {
        keys = append(keys, key)
    }
    sort.Ints(keys)	
    return keys
}

//重新分配组和分片
func DistributionGroups(config *Config) {
	keys := SortGroup(config.Groups)
	for index := 0; index < NShards; {
		for i:=0;i<len(keys);i++ {
		    key := keys[i]
			config.Shards[index] = key
			index++
			if index >= NShards {
				break
			}
		}
	}
}

//获得某组下单分片
func GetCountGroup(Shards *[NShards]int, group int) (rst []int) {
	for i := 0; i < len(*Shards); i++ {
		if (*Shards)[i] == group {
			rst = append(rst, i)
		}
	}
	return
}

//获得每个组下的分片
func GetCountShards(config *Config)  map[int][]int {
	rst := make( map[int][]int)
	for key,_ := range config.Groups {
		rst[key] = make([]int, 0)
	} 
	for i := 0; i < len(config.Shards); i++ {
		group := config.Shards[i]
		value, ok := rst[group]
		if ok {
			value = append(value, i)
		} else {
			value = make([]int, 1)
			value[0] = i
		}
		rst[group] = value
	}
	return rst
}

//排序
func SortCountShards(shards map[int][]int)  []int {
    var keys []int
    for key,_ := range shards {
        keys = append(keys, key)
    }
    sort.Ints(keys)	
    return keys
}

//获取分片数量最多组
func GetMaxCountShards(config *Config)(group int,rst []int) {
	maps := GetCountShards(config)
	keys := SortCountShards(maps)
	max := 0
	for i:=0;i<len(keys);i++ {
		key := keys[i]
		value := maps[key]
		if (len(value) > max) {
			group = key
			rst = value
			max = len(value)
		}
	}
	return
}

//获取分片数量最少组
func GetMinCountShards(config *Config, without int) int {
	rst := 0
	maps := GetCountShards(config)
	keys := SortCountShards(maps)
	min := NShards + 1

	for i:=0;i<len(keys);i++ {
		key := keys[i]
		value := maps[key]
		if key == without {
			continue
		}
		if len(value) < min {
			rst = key
			min = len(value)
		}
	}
	return rst
}

//获取分组分片信息字符串
func GetSlientInfoString(Shards []int) string {
	rst := "shards :"
	for i := 0; i < len(Shards); i++ {
		rst += strconv.Itoa(i)
		rst += "-"
		rst += strconv.Itoa(Shards[i])
		rst += ", "
	}
	return rst
}

//获取分组分片信息字符串
func GetShardsInfoString(Shards *[NShards]int) string {
	rst := "shards :"
	for i := 0; i < len(Shards); i++ {
		rst += strconv.Itoa(i)
		rst += "-"
		rst += strconv.Itoa(Shards[i])
		rst += ", "
	}
	return rst
}

//获取分组分片信息字符串
func GetShardsCountInfoString(config *Config) string {
	return GetGroupCountsInfoString(GetCountShards(config))
}

//获取分组分片信息字符串
func GetGroupCountsInfoString(groups map[int][]int) string {
	info := ""
	for key, value := range groups {
		info += "group "
		info += strconv.Itoa(key)
		info += " : "
		for i := 0; i < len(value); i++ {
			info += strconv.Itoa(value[i])
			info += ", "
		}
		info += "\n"
	}
	return info
}

//获取组信息字符串
func GetGroupsInfoString(groups map[int][]string) string {
	info := "group :"
	for key, _ := range groups {
		info += strconv.Itoa(key)
		info += ", "
	}
	return info
}
