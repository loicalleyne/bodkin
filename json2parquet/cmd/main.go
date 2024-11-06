package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"

	"github.com/redpanda-data/benthos/v4/public/bloblang"

	"github.com/loicalleyne/bodkin"
	j2p "github.com/loicalleyne/bodkin/json2parquet"
)

var exe *bloblang.Executor
var cpuprofile = flag.String("cpuprofile", "default.pgo", "write cpu profile to `file`")

func main() {
	inferMode := flag.Bool("infer_timeunits", true, "Infer date, time and timestamps from strings")
	quotedValuesAreStrings := flag.Bool("quoted_values_are_strings", false, "Treat quoted bool, float and integer values as strings")
	withTypeConversion := flag.Bool("type_conversion", false, "upgrade field types if data changes")
	inputFile := flag.String("in", "t.json", "input file")
	outputFile := flag.String("out", "screens.parquet", "output file")
	dryRun := flag.Bool("n", false, "only print the schema")
	lines := flag.Int64("lines", 0, "number of lines from which to infer schema; 0 means whole file is scanned")
	flag.Parse()
	if *inputFile == "" {
		log.Fatal("no input file specified")
	}
	log.Println("detecting schema")
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
		defer log.Printf("program ended\nto view profile run 'go tool pprof -http localhost:8080 %s\n", *cpuprofile)
	}
	var opts []bodkin.Option
	if *inferMode {
		opts = append(opts, bodkin.WithInferTimeUnits())
	}
	if *withTypeConversion {
		opts = append(opts, bodkin.WithTypeConversion())
	}
	if *quotedValuesAreStrings {
		opts = append(opts, bodkin.WithQuotedValuesAreStrings())
	}
	if *lines != 0 {
		opts = append(opts, bodkin.WithMaxCount(*lines))
	}
	arrowSchema, n, err := j2p.SchemaFromFile(*inputFile, opts...)
	if err == bodkin.ErrInvalidInput {
		fmt.Printf("schema creation error %v\n", err)
	}
	if arrowSchema == nil {
		log.Fatal("nil schema")
	}
	log.Printf("schema from %d records\n", n)
	fmt.Println(arrowSchema.String())
	if !*dryRun {
		if *outputFile == "" {
			log.Fatal("no output file specified")
		}
		log.Println("starting conversion to parquet")

		n, err = j2p.RecordsFromFile(*inputFile, *outputFile, arrowSchema, nil)
		log.Printf("%d records written", n)
		if err != nil {
			log.Printf("parquet error: %v", err)
		}
	}
}
