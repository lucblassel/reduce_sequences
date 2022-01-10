package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	red "github.com/lucblassel/reduction-functions"
	// "github.com/pkg/profile"
	"github.com/schollz/progressbar/v3"
)

type Args struct {
	offsets   string
	output    string
	preserve  bool
	reduction string
	sequences string
	threads   int
}

func ParseArgs() Args {
	reductionPath := flag.String("reduction", "", "path to reduction function .json file")
	sequencesPath := flag.String("sequences", "", "path to sequences to reduce in .fasta format")
	outputPath := flag.String("output", "", "path to output reduced sequences in .fasta format")
	threads := flag.Int("threads", 1, "number of threads to use (default = 1)")
	offsets := flag.String("offsets", "", "path to save correspondence between original positions and reduced positions (optional)")
	preserve := flag.Bool("preserve", false, "preserve order of sequences from original fasta file")

	flag.Parse()

	if *reductionPath == "" || *sequencesPath == "" || *outputPath == "" {
		usage := `
This is intended apply a reduction function to a set of sequences:

	reduceFasta \
		--reduction <path/to.json> \ 
		--sequences <path/to.fasta> \
		--output <path/to.fasta> \
		--offsets <path/to.json> \
		--threads <number of threads> \
		--preserve

Options:
`
		fmt.Print(usage)
		flag.PrintDefaults()
		os.Exit(1)
	}

	return Args{
		offsets:   *offsets,
		output:    *outputPath,
		preserve:  *preserve,
		reduction: *reductionPath,
		sequences: *sequencesPath,
		threads:   *threads,
	}

}

func main() {

	args := ParseArgs()

	var mapping map[string]string

	err := red.CheckSurjectionFile(args.reduction, &mapping)
	if err != nil {
		log.Fatalf("Could not parse reduction function file: %v", err)
	}

	inputRecords, err := parseFasta(args.sequences)
	if err != nil {
		log.Fatalf("Error reading FASTA: %v", err)
	}

	bulkJobs := make([]Job, len(inputRecords))
	names := make([]string, len(inputRecords))

	if args.offsets != "" {
		reduction := red.MakeReductionFunctionBitVectorDeleteAmbs(mapping)
		for i, record := range inputRecords {
			names[i] = record.Name
			bulkJobs[i] = JobWithOffset{
				Record:  record,
				Reducer: reduction,
			}
		}
	} else {
		reduction := red.MakeReductionFunctionDeleteAmbs(mapping)
		for i, record := range inputRecords {
			names[i] = record.Name
			bulkJobs[i] = JobNoOffset{
				Record:  record,
				Reducer: reduction,
			}
		}
	}

	bar := progressbar.Default(int64(len(inputRecords)))

	pool := New(args.threads)

	go pool.Generate(bulkJobs)
	go pool.Run()

	outputFasta, err := os.Create(args.output)
	if err != nil {
		log.Fatalf("Couldn't create output FASTA: %v", err)
	}

	// Keep records and reduced sequences
	offsetRecords := make([]*Record, 0)
	seqs := make(map[string]*string, len(names))

	// Open Fasta writer to write output sequences
	defer outputFasta.Close()

	// Read results from workerPool
	for result := range pool.results {

		if !args.preserve {
			WriteWrapped(result.Name, result.sequence, 80, outputFasta)
		} else {
			seqs[result.Name] = &result.sequence
		}

		if args.offsets != "" {
			offsetRecords = append(offsetRecords, result)
		}
		bar.Add(1)
	}

	// Write sequences in order
	if args.preserve {
		for _, name := range names {
			WriteWrapped(name, *seqs[name], 80, outputFasta)
		}
	}

	// Write offsets
	if args.offsets != "" {
		offsetFile, err := os.Create(args.offsets)
		if err != nil {
			log.Fatalf("Could not create offset file: %v", err)
		}
		defer offsetFile.Close()

		enc := json.NewEncoder(offsetFile)
		err = enc.Encode(offsetRecords)
		if err != nil {
			log.Fatalf("Error encoding offsets: %v", err)
		}
	}
}
