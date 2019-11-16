package mapreduce

import (
	"encoding/json"
	"log"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string) {

	//相同key的集合
	kvsMap := make(map[string]([]string))

	//遍历读reduce文件
	for i := 0; i < nMap; i++ {
		name := reduceName(jobName, i, reduceTask)
		file, err := os.Open(name)
		if nil != err {
			log.Fatal(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			//合并相同key的数据
			kvsMap[kv.Key] = append(kvsMap[kv.Key], kv.Value)
		}
		file.Close()
	}

	//创建reduce合并结果文件
	reduceFile, err := os.Create(outFile)
	if nil != err {
		log.Fatal(err)
	}
	enc := json.NewEncoder(reduceFile)
	//调用reduce合并相同key并写入文件
	for key, value := range kvsMap {
		data := reduceF(key, value)
		kv := KeyValue{key, data}
		enc.Encode(kv)
	}
	reduceFile.Close()
}
