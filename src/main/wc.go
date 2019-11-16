package main

import (
	"fmt"
	"mapreduce"
	"os"
	"strconv"
	"strings"
	"unicode"
)

func mapF(filename string, contents string) []mapreduce.KeyValue {
	//单词频率结果
	wordsKv := make(map[string]int)
	//分词并遍历
	words := strings.FieldsFunc(contents, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	for _, word := range words {
		//统计单词
		if _, ok := wordsKv[word]; ok {
			wordsKv[word]++
		} else {
			wordsKv[word] = 1
		}
	}
	//转换为输出格式
	var rst []mapreduce.KeyValue
	for key, value := range wordsKv {
		kv := mapreduce.KeyValue{
			key,
			strconv.Itoa(value),
		}
		rst = append(rst, kv)
	}
	return rst
}

func reduceF(key string, values []string) string  {
	cnt := 0
    //合并统计结果
	for _, value := range values {
		num, err := strconv.Atoi(value)
		if err != nil {
			break
		}
		cnt += num
	}
	return strconv.Itoa(cnt)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100, nil)
	}
}
