package main

import (
  "fmt"
  "golang.org/x/net/context"

  elastic "gopkg.in/olivere/elastic.v5"
  "flag"
  "os"
  "encoding/csv"
  "github.com/ctessum/macreader"
  "bufio"
  //"time"
  "io"
  "encoding/json"
)

func main() {
  // 解析命令行输入
  host := flag.String("host", "http://localhost:9200", "host, e.g. http://localhost:9200")
  file := flag.String("file", "", "file path")
  esIndex := flag.String("index", "", "elastic search index")
  esType := flag.String("type", "", "elastic search type")
  flag.Parse()
  if *file == "" {
    fmt.Println("please set which csv file you want to import clearly")
    return
  }
  if *esIndex == "" {
    fmt.Println("please set elastic search index")
    return
  }
  if *esType == "" {
    fmt.Println("please set elastic search type")
    return
  }

  // 连接es
  ctx := context.Background()
  client, err := elastic.NewClient(
    elastic.SetURL(*host),
    elastic.SetSniff(false))
  if err != nil {
    panic(err)
  }

  // 检查index是否存在，如果不存在则创建index
  exists, err := client.IndexExists(*esIndex).Do(ctx)
  if err != nil {
    panic(err)
  }
  if !exists {
    _, err := client.CreateIndex(*esIndex).Do(ctx)
    if err != nil {
      panic(err)
    }
  }

  // 解析csv
  f, _ := os.Open(*file)
  r := csv.NewReader(macreader.New(bufio.NewReader(f)))
  keys, err := r.Read()
  // start := time.Now().Unix()
  bulkRequest := client.Bulk()
  for {
    record, err := r.Read()
    if err == io.EOF {
      break
    }
    m := make(map[string]string)
    for i, key := range keys {
      m[key] = record[i]
    }
    jsonStr, err := json.Marshal(m)
    if err != nil {
      panic(err)
    }
    req := elastic.NewBulkIndexRequest().Index(*esIndex).Type(*esType).Doc(string(jsonStr))
    bulkRequest.Add(req)
  }

  bulkResponse, err := bulkRequest.Do(ctx)
  if err != nil {
  }
  indexed := bulkResponse.Indexed()
  fmt.Println("导入了",len(indexed),"条数据")
  // end := time.Now().Unix()
  // fmt.Println("耗时", end - start)
}