package main

import (
"github.com/hillbig/rsdic"
)

type Record struct {
	Name     string `json:"name"`
	Offsets  []byte `json:"offsets"`
	sequence string
}

type JobNoOffset struct {
	Record *Record
	Reducer func(string) string
}

type JobWithOffset struct {
	Record *Record
	Reducer func(string) (string, *rsdic.RSDic)
}

// Do not keep offsets
func (job JobNoOffset) execute() (*Record, error) {
	return &Record{
		Name: job.Record.Name,
		sequence: job.Reducer(job.Record.sequence),
	}, nil
}

// Keep offsets of sequences as bitvector
func (job JobWithOffset) execute() (*Record, error) {

	sequence, offsets := job.Reducer(job.Record.sequence)
	bytes, err := offsets.MarshalBinary()

	if err != nil {
		return nil, err
	}

	return &Record{
		Name: job.Record.Name,
		Offsets: bytes,
		sequence: sequence,
	}, nil
}


