package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	pilosa "github.com/pilosa/go-pilosa"
)

type KruxBitIterator struct {
	reader      io.Reader
	line        int
	scanner     *bufio.Scanner
	mapper      *SegmentIDMapper
	viewerIndex uint64
	viewerValue string
}

type SegmentIDMapper struct {
	client *pilosa.Client
	field  *pilosa.Field
	cache  map[string]uint64
	rowcnt uint64
}

func (mapper *SegmentIDMapper) GetID(kruxid string) uint64 {
	if value, ok := mapper.cache[kruxid]; ok {
		return value
	}
	query := mapper.field.FilterFieldTopN(1, nil, "kruxid", kruxid)
	response, err := mapper.client.Query(query, nil)
	if err != nil {
		log.Fatal(err)
	}
	result := response.Result()
	for _, ci := range result.CountItems() {
		mapper.cache[kruxid] = ci.ID
		return ci.ID
	}

	// if doesn't exist, then we need to make a new row / attribute
	rowid := mapper.rowcnt
	mapper.rowcnt++
	mapper.cache[kruxid] = rowid
	attrs := make(map[string]interface{})
	attrs["kruxid"] = kruxid
	aquery := mapper.field.SetRowAttrs(rowid, attrs)
	response, err = mapper.client.Query(aquery, nil)
	if err != nil {
		log.Fatal(err)
	}

	return rowid
}

func NewSegmentIDMapper(client *pilosa.Client, field *pilosa.Field) *SegmentIDMapper {
	q := field.TopN(10000000)
	r, err := client.Query(q, nil)
	if err != nil {
		log.Fatal(err)
	}
	rows := len(r.Result().CountItems())

	return &SegmentIDMapper{
		client: client,
		field:  field,
		cache:  make(map[string]uint64),
		rowcnt: uint64(rows),
	}
}

func NewKruxBitIterator(reader io.Reader, mapper *SegmentIDMapper, viewerIndex uint64) *KruxBitIterator {
	return &KruxBitIterator{
		reader:      reader,
		line:        0,
		viewerValue: "",
		viewerIndex: viewerIndex,
		scanner:     bufio.NewScanner(reader),
		mapper:      mapper,
	}
}

func (c *KruxBitIterator) NextRecord() (pilosa.Record, error) {
	if ok := c.scanner.Scan(); ok {
		c.line++
		text := strings.TrimSpace(c.scanner.Text())
		parts := strings.Split(text, "^-^")
		if len(parts) < 2 {
			return pilosa.Column{}, fmt.Errorf("Invalid Krux line: %d", c.line)
		}

		if parts[0] != c.viewerValue {
			c.viewerIndex++
			c.viewerValue = parts[0]
		}
		bit := pilosa.Column{
			RowID:    c.mapper.GetID(parts[1]),
			ColumnID: c.viewerIndex,
		}
		return bit, nil
	}
	if err := c.scanner.Err(); err != nil {
		log.Fatal("Error encountered reading file", err)
		return pilosa.Column{}, err
	}
	return pilosa.Column{}, io.EOF
}

func eat(client *pilosa.Client, field *pilosa.Field, mapper *SegmentIDMapper, path string, viewerIndex uint64) uint64 {
	log.Println("Attempting to ingest", path)
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	iterator := NewKruxBitIterator(gr, mapper, viewerIndex)
	err = client.ImportField(field, iterator, pilosa.OptImportBatchSize(1000000))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("done")
	return iterator.viewerIndex
}

func main() {
	sindex := flag.String("index", "segmentation", "name of the pilosa index")
	sfield := flag.String("field", "membership", "name of the pilosa field")
	dir := flag.String("dir", ".", "directory location to search for gzip files")
	suri := flag.String("uri", "localhost:10101", "uri location of the pilosa server")
	flag.Parse()

	files, _ := filepath.Glob(*dir + "/*.gz")
	uri, err := pilosa.NewURIFromAddress(*suri)
	if err != nil {
		log.Fatal(err)
	}

	client, err := pilosa.NewClient(uri)
	if err != nil {
		log.Fatal(err)
	}
	schema := pilosa.NewSchema()
	index := schema.Index(*sindex)
	field := index.Field(*sfield)
	err = client.SyncSchema(schema)
	if err != nil {
		log.Fatal(err)
	}

	mapper := NewSegmentIDMapper(client, field)
	viewerIndex := uint64(0)
	for i := 0; i < len(files); i++ {
		viewerIndex = eat(client, field, mapper, files[i], viewerIndex)
		log.Println("Ingested", viewerIndex, "viewers")
	}
}
