package main

import (
	"fmt"
	"io"
	"os"

	mgo "github.com/mongodb-labs/mongoreplay/internal/llmgo"
	"github.com/mongodb-labs/mongoreplay/internal/llmgo/bson"
	"github.com/mongodb-labs/mongoreplay/mongoreplay"
)

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

		//
		if op.OpCode() != mongoreplay.OpCodeMessage {
			bsonToWriter(playbackWriter, op)
			continue
		}

		rOp, err := op.Parse()
		if err != nil {
			panic(err)
		}

		mOp, ok := rOp.(*mongoreplay.MsgOp)
		if !ok {
			panic("not query op")
		}

		// Replacement sections
		newSections := make([]mgo.MsgSection, 0, len(mOp.Sections))

		// A message op has a list of sections- https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/#std-label-wire-msg-sections
		for section := range mOp.Sections {
			sectionData := mOp.Sections[section]
			if sectionData.PayloadType == mgo.MsgPayload0 {
				// This is a BSON Objet according to the spec
				mR, ok := sectionData.Data.(*bson.Raw)
				if !ok {
					panic("not *bson.Raw")
				}

				var bsonDoc bson.M
				if err := mR.Unmarshal(&bsonDoc); err != nil {
					panic(fmt.Sprintf("unmarshalling into bsonDoc: %v", err))
				}
				fmt.Printf("Original BSON:\n%s\n", bsonDoc)
				delete(bsonDoc, "$clusterTime")
				fmt.Printf("Modified BSON:\n%s\n", bsonDoc)
				newSections = append(newSections, mgo.MsgSection{
					PayloadType: sectionData.PayloadType,
					Data:        bsonDoc,
				})

			}
		}
		// Now we have modified the bsonDoc, we need to write it back to the op

		fmt.Printf("Header: %s\n", mOp.Header)
		fmt.Printf("FlagBits: %d\n", mOp.Flags)
		fmt.Printf("Sections: %d\n", mOp.Sections[0].Data)
		fmt.Printf("Checksum: %d\n", mOp.Checksum)

		mBytes, err := bson.Marshal(mOp.MsgOp)
		if err != nil {
			panic(err)
		}

		msgLen := len(mOp.Header.ToWire()) + len(mBytes)
		mOp.Header.MessageLength = int32(msgLen)

		buf := make([]byte, 0, 256)
		buf = addHeader(buf, 2004)
		buf = addInt32(buf, int32(mOp.Flags))
		buf, err = addBSON(buf, newSections)
		if err != nil {
			panic(err)
		}
		// Set new message length
		setInt32(buf, 0, int32(len(buf)))

		op.RawOp = mongoreplay.RawOp{
			Header: mOp.Header,
			Body:   buf,
		}

		bsonToWriter(playbackWriter, op)

		break
	}
}
