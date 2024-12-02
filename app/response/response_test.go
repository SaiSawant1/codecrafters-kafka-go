package response

import (
	"bytes"
	"encoding/binary"
	"log"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/app/request"
	"github.com/gofrs/uuid"
)

func TestNewResponse(t *testing.T) {
	var desiredResponse bytes.Buffer

	binary.Write(&desiredResponse, binary.BigEndian, uint32(41))
	binary.Write(&desiredResponse, binary.BigEndian, uint32(7))
	binary.Write(&desiredResponse, binary.BigEndian, uint8(0))

	binary.Write(&desiredResponse, binary.BigEndian, uint32(0))

	binary.Write(&desiredResponse, binary.BigEndian, uint8(2))
	binary.Write(&desiredResponse, binary.BigEndian, uint16(0x0003))

	binary.Write(&desiredResponse, binary.BigEndian, uint8(4))
	desiredResponse.WriteString("foo")

	nullUUID := uuid.UUID{}
	binary.Write(&desiredResponse, binary.BigEndian, nullUUID.Bytes())
	binary.Write(&desiredResponse, binary.BigEndian, uint8(0x00))
	binary.Write(&desiredResponse, binary.BigEndian, uint8(0x01))
	binary.Write(&desiredResponse, binary.BigEndian, uint32(0x00000df8))

	binary.Write(&desiredResponse, binary.BigEndian, uint8(0))
	binary.Write(&desiredResponse, binary.BigEndian, uint8(0xff))
	binary.Write(&desiredResponse, binary.BigEndian, uint8(0))

	testReq := request.Request{
		ApiKey:        uint16(75),
		ApiVersion:    uint16(0),
		CorrelationID: uint32(7),
		ClientId:      "kafka-cli",
		TopicArray:    []request.Topic{request.Topic{TopicName: "foo"}},
	}

	res, err := CreateResponse(testReq)
	if err != nil {
		t.FailNow()
		return
	}

	log.Println(res)
	log.Println(desiredResponse.Bytes())
	if !bytes.Equal(res, desiredResponse.Bytes()) {
		log.Println("res and desiredResponse not same")
		t.FailNow()
	}
}
