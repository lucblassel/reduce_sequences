package main

import (
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/biogo/biogo/alphabet"
	"github.com/biogo/biogo/io/seqio"
	"github.com/biogo/biogo/io/seqio/fasta"
	"github.com/biogo/biogo/seq/linear"
	"github.com/ulikunitz/xz"
)

func getReader(filename string) (io.Reader, error) {
	ext := filepath.Ext(filename)
	fileContent, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	switch ext {
	case ".gz":
		return gzip.NewReader(fileContent)
	case ".bz2":
		return bzip2.NewReader(fileContent), nil
	case ".xz":
		return xz.NewReader(fileContent)
	default:
		return fileContent, err
	}
}

func parseFasta(filename string) ([]*Record, error) {
	records := make([]*Record, 0)

	data, err := getReader(filename)
	if err != nil {
		return records, err
	}

	template := linear.NewSeq("", nil, alphabet.DNA)
	fastaReader := fasta.NewReader(data, template)
	scanner := seqio.NewScanner(fastaReader)

	for scanner.Next() {
		s := scanner.Seq().(*linear.Seq)
		records = append(records, &Record{Name: s.ID, sequence: s.Seq.String()})
	}

	return records, nil
}

func WriteFastaSeq(seq *linear.Seq, writer *fasta.Writer) {
	_, err := writer.Write(seq)
	if err != nil {
		log.Fatalf("Error writing reduced sequence: %v", err)
	}
}

func WriteSeq(name string, seq *string, writer io.Writer) {
	_, err := fmt.Fprintf(writer, ">%s\n", name)
	if err != nil {
		log.Fatalf("Error writing reduced sequence name: %v", err)
	}
	_, err = fmt.Fprintf(writer, "%s\n", *seq)
	if err != nil {
		log.Fatalf("Error writing reduced sequence: %v", err)
	}
}

func WriteWrapped(name string, seq string, width int, writer io.Writer) {
	var builder strings.Builder

	builder.WriteString(fmt.Sprintf(">%s\n", name))
	for i := 0; i < len(seq); i += width {
		end := i + width
		if end > len(seq) {
			end = len(seq)
		}
		builder.WriteString(seq[i:end])
		builder.WriteString("\n")
	}
	fmt.Fprint(writer, builder.String())
}
