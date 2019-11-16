package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue) {

	//读取处理数据
	data, err := ioutil.ReadFile(inFile)
	if nil != err {
		log.Fatal(err)
	}
	//调用map函数获得kv集合
	kvs := mapF(inFile, string(data))

	//创建nReduce 数量文件
	var outFiles []*os.File
	defer func() {
		for _, file := range outFiles {
			file.Close()
		}
	}()
	for i := 0; i < nReduce; i++ {
		name := reduceName(jobName, mapTask, i)
		file, err := os.Create(name)
		if nil != err {
			log.Fatal(err)
		}
		outFiles = append(outFiles, file)
	}

	//kv集合写入相应文件
	for _, kv := range kvs {
		index := ihash(kv.Key) % nReduce
		enc := json.NewEncoder(outFiles[index])
		enc.Encode(kv)
	}

}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
