package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/goccy/go-json"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

// jcleaner takes as input a JSONL file, and removes all null fields, empty arrays,
// empty objects and empty strings.
func main() {
	inputFile := flag.String("in", "", "input file")
	outputFile := flag.String("out", "", "output file")
	flag.Parse()
	if *inputFile == "" {
		log.Fatal("no input file specified")
	}
	if *outputFile == "" {
		log.Fatal("no output file specified")
	}
	problemLines := fileNameWithoutExt(*outputFile) + "_problem.json"
	f, err := os.Open(*inputFile)
	if err != nil {
		panic(err)
	}
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(err)
		}
	}()
	defer f.Close()
	bloblangMapping := `map remove_null_empty {
		root = match {
		  (this.type() == "object" && this.length() == 0)  => deleted()
		  this.type() == "object" => this.map_each(i -> i.value.apply("remove_null_empty"))
		  (this.type() == "array" && this.length() == 0)  => deleted()
		  this.type() == "array" => this.map_each(v -> v.apply("remove_null_empty"))
		  this.type() == "null" => deleted()
		  this.type() == "string" && this.length() == 0 => deleted()
		  }
		}
	  root = this.apply("remove_null_empty")`
	exe, err := bloblang.Parse(bloblangMapping)
	if err != nil {
		log.Println(err)
	}

	nf, err := os.Create(*outputFile)
	if err != nil {
		panic(err)
	}
	defer nf.Close()
	w := bufio.NewWriterSize(nf, 1024*4)

	pf, err := os.Create(problemLines)
	if err != nil {
		panic(err)
	}
	defer pf.Close()
	pw := bufio.NewWriterSize(nf, 1024*4)

	r := bufio.NewReaderSize(f, 1024*4)
	s := bufio.NewScanner(r)
	newline := []byte("\n")
	for s.Scan() {
		y := s.Bytes()
		b, err := ApplyBloblangMapping(y, exe)
		if err != nil {
			pw.Write(y)
			pw.Write(newline)
			continue
		}
		_, err = w.Write(b)
		if err != nil {
			pw.Write(y)
			pw.Write(newline)
			continue
		}
		w.Write(newline)
	}
	w.Flush()
}

func ApplyBloblangMapping(jsonInput []byte, exe *bloblang.Executor) ([]byte, error) {
	// Parse the JSON input into a map[string]interface{}
	var inputMap map[string]interface{}
	if err := json.Unmarshal(jsonInput, &inputMap); err != nil {
		return nil, err
	}

	// Execute the Bloblang mapping
	res, err := exe.Query(inputMap)
	if err != nil {
		return nil, err
	}

	// Convert the result back into a JSON string
	jsonResult, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}

	return jsonResult, nil
}

func fileNameWithoutExt(fileName string) string {
	return fileName[:len(fileName)-len(filepath.Ext(fileName))]
}
