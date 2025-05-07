package main

import (
	"fmt"
	"io"
	"os"
	"slices"

	mgo "github.com/mongodb-labs/mongoreplay/internal/llmgo"
	"github.com/mongodb-labs/mongoreplay/internal/llmgo/bson"
	"github.com/mongodb-labs/mongoreplay/mongoreplay"
)

// Most of the following functions are just copied from private functions in mongoreplay.

// bsonFromReader reads a bson document from the reader into out.
func bsonFromReader(reader io.Reader, out interface{}) error {
	buf, err := mongoreplay.ReadDocument(reader)
	if err != nil {
		if err != io.EOF {
			err = fmt.Errorf("ReadDocument Error: %v", err)
		}
		return err
	}
	err = bson.Unmarshal(buf, out)
	if err != nil {
		return fmt.Errorf("unmarshal recordedOp error: %v", err)
	}
	return nil
}

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

func addUint32(b []byte, i uint32) []byte {
	return append(b, byte(i), byte(i>>8), byte(i>>16), byte(i>>24))
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
	origFile, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println("origFile open error")
		panic(err)
	}

	defer origFile.Close()

	fixedFile, err := os.Create(os.Args[2])
	if err != nil {
		fmt.Println("fixedFile reate error")
		panic(err)
	}
	defer fixedFile.Close()

	metadata := new(mongoreplay.PlaybackFileMetadata)
	err = bsonFromReader(origFile, metadata)
	if err != nil {
		panic(fmt.Errorf("bson read error: %v", err))
	}

	bsonToWriter(fixedFile, metadata)

	var opCount int
	var modifiedCount int
	var compressedCount int

	for {
		opCount++
		op := new(mongoreplay.RecordedOp)
		err = bsonFromReader(origFile, op)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(fmt.Errorf("bson read error: %v", err))
		}

		compressed := op.OpCode() == mongoreplay.OpCodeCompressed
		if compressed {
			compressedCount++
		}
		rawOp, err := op.Parse()
		if err != nil {
			fmt.Println(opCount)
			panic(fmt.Errorf("parsing op: %v", err))
		}
		if rawOp.OpCode() != mongoreplay.OpCodeMessage {
			bsonToWriter(fixedFile, op)
			continue
		}

		msgOp, ok := rawOp.(*mongoreplay.MsgOp)
		if !ok {
			// Happens with things like MsgOpGetMore
			bsonToWriter(fixedFile, op)
			continue
		}

		buf := make([]byte, 0, 256)
		buf = addHeader(buf, 2013)
		buf = addInt32(buf, int32(msgOp.Flags))

		for i := range msgOp.Sections {
			switch msgOp.Sections[i].PayloadType {
			case mgo.MsgPayload0:
				buf = append(buf, byte(0))

				secRaw, ok := msgOp.Sections[i].Data.(*bson.Raw)
				if !ok {
					panic("Not *bson.Raw")
				}

				var secBSON bson.D
				if err := secRaw.Unmarshal(&secBSON); err != nil {
					panic(fmt.Errorf("unmarshalling section bson: %v", err))
				}
				secBSON = slices.DeleteFunc(secBSON, func(e bson.DocElem) bool {
					if e.Name == "$clusterTime" {
						modifiedCount++
						return true
					}
					return false
				})

				marshalled, err := bson.Marshal(secBSON)
				if marshalled[len(marshalled)-1] != 0x00 {
					panic("P0: doesn't end in null byte")
				}
				if err != nil {
					panic(fmt.Errorf("marshalling section bson: %v", err))
				}
				buf = append(buf, marshalled...)
			case mgo.MsgPayload1:
				buf = append(buf, byte(1))
				payload, ok := msgOp.Sections[i].Data.(mgo.PayloadType1)
				if !ok {
					panic("incorrect type given for payload")
				}

				// Write out the size
				currentOffset := len(buf)
				// set temp size
				buf = addInt32(buf, 0)

				// Write out the identifier
				buf = addCString(buf, payload.Identifier)

				payloadSize := 0
				// Write out the docs
				for _, d := range payload.Docs {
					docRaw, ok := d.(bson.Raw)
					if !ok {
						panic("Not *bson.Raw")
					}

					var docBSON bson.D
					if err := docRaw.Unmarshal(&docBSON); err != nil {
						panic(fmt.Errorf("unmarshalling section bson: %v", err))
					}
					docBSON = slices.DeleteFunc(docBSON, func(e bson.DocElem) bool {
						if e.Name == "$clusterTime" {
							modifiedCount++
							return true
						}
						return false
					})

					marshalled, err := bson.Marshal(docBSON)
					if err != nil {
						panic(fmt.Errorf("marshalling section bson: %v", err))
					}
					buf = append(buf, marshalled...)
					if marshalled[len(marshalled)-1] != 0x00 {
						panic("P1: doesn't end in null byte")
					}
					payloadSize += len(marshalled)
				}

				// Overwrite correct size
				setInt32(buf, currentOffset, int32(4+len(payload.Identifier)+1+payloadSize))
			}
		}

		setInt32(buf, 0, int32(len(buf)))
		op.Header.MessageLength = int32(len(buf))

		// if compressed {
		// 	buf, err = mgo.CompressMessage(buf)
		// 	if err != nil {
		// 		panic(fmt.Errorf("recompressing message: %v", err))
		// 	}
		// }
		op.Body = buf

		bsonToWriter(fixedFile, op)
	}

	fmt.Println("Total Ops: ", opCount)
	fmt.Println("Modified: ", modifiedCount)
	fmt.Println("Compressed: ", compressedCount)
}
