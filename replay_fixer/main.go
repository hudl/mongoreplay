package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/mongodb-labs/mongoreplay/internal/llmgo/bson"
	"github.com/mongodb-labs/mongoreplay/mongoreplay"
)

type queryWrapper struct {
	Query bson.D `bson:"$query"`
}

// Most of the following functions are just copied from private functions in mongoreplay.

// bsonToWriter writes a bson document to the writer given.
func bsonToWriter(writer io.Writer, in interface{}) error {
	bsonBytes, err := bson.Marshal(in)
	if err != nil {
		return err
	}
	_, err = writer.Write(bsonBytes)
	if err != nil {
		return err
	}
	return nil
}

var emptyHeader = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

func addHeader(b []byte, opcode int) []byte {
	i := len(b)
	b = append(b, emptyHeader...)
	// Enough for current opcodes.
	b[i+12] = byte(opcode)
	b[i+13] = byte(opcode >> 8)
	return b
}

func addInt32(b []byte, i int32) []byte {
	return append(b, byte(i), byte(i>>8), byte(i>>16), byte(i>>24))
}

func addInt64(b []byte, i int64) []byte {
	return append(b, byte(i), byte(i>>8), byte(i>>16), byte(i>>24),
		byte(i>>32), byte(i>>40), byte(i>>48), byte(i>>56))
}

func addCString(b []byte, s string) []byte {
	b = append(b, []byte(s)...)
	b = append(b, 0)
	return b
}

func addBSON(b []byte, doc interface{}) ([]byte, error) {
	if doc == nil {
		return append(b, 5, 0, 0, 0, 0), nil
	}
	data, err := bson.Marshal(doc)
	if err != nil {
		return b, err
	}
	return append(b, data...), nil
}

func setInt32(b []byte, pos int, i int32) {
	b[pos] = byte(i)
	b[pos+1] = byte(i >> 8)
	b[pos+2] = byte(i >> 16)
	b[pos+3] = byte(i >> 24)
}

func main() {
	reader, err := mongoreplay.NewPlaybackFileReader(os.Args[1], false)
	if err != nil {
		fmt.Println("newplaybackfilereader error")
		panic(err)
	}

	if reader == nil {
		panic("nil reader")
	}

	playbackWriter, _ := mongoreplay.NewPlaybackFileWriter(os.Args[2], false, false)
	defer playbackWriter.Close()

	opCh, _ := reader.OpChan(1)

	for op := range opCh {
		// Don't do anything with non Query ops, just write them straight back out to the modified bson file.
		if op.OpCode() != mongoreplay.OpCodeQuery {
			bsonToWriter(playbackWriter, op)
			continue
		}

		rOp, err := op.Parse()
		if err != nil {
			panic(err)
		}

		qOp, ok := rOp.(*mongoreplay.QueryOp)
		if !ok {
			panic("not query op")
		}

		qR, ok := qOp.Query.(*bson.Raw)
		if !ok {
			panic("not *bson.Raw")
		}

		var qMap queryWrapper
		if err := qR.Unmarshal(&qMap); err != nil {
			panic(fmt.Sprintf("unmarshalling into qMap: %s", err))
		}

		// Some commands don't have a "$query" wrapper (like updates), so just skip 'em.
		if qMap.Query == nil {
			continue
		}

		qOp.Query = qMap.Query
		interJSONQ, _ := mongoreplay.ConvertBSONValueToJSON(qR)
		jsonQ, _ := json.Marshal(interJSONQ)

		fmt.Printf("Modified query from:\n%s\n", jsonQ)

		interJSONQ, _ = mongoreplay.ConvertBSONValueToJSON(qOp.Query)
		jsonQ, _ = json.Marshal(interJSONQ)
		fmt.Printf("to:\n%s\n\n", jsonQ)

		qBytes, err := bson.Marshal(qOp.Query)
		if err != nil {
			panic(err)
		}

		msgLen := len(qOp.Header.ToWire()) + len(qBytes)
		qOp.Header.MessageLength = int32(msgLen)

		buf := make([]byte, 0, 256)
		buf = addHeader(buf, 2004)
		buf = addInt32(buf, int32(qOp.Flags))
		buf = addCString(buf, qOp.Collection)
		buf = addInt32(buf, qOp.Skip)
		buf = addInt32(buf, qOp.Limit)
		buf, err = addBSON(buf, qOp.Query)
		if err != nil {
			panic(err)
		}

		// Set new message length
		setInt32(buf, 0, int32(len(buf)))

		op.RawOp = mongoreplay.RawOp{
			Header: qOp.Header,
			Body:   buf,
		}

		bsonToWriter(playbackWriter, op)

	}
}
