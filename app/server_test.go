package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/app/request"
	"github.com/codecrafters-io/kafka-starter-go/app/response"
	"github.com/gofrs/uuid"
)

func ConstructEncodedMessage() bytes.Buffer {
	TestReq := request.Request{
		ApiKey:        75,
		ApiVersion:    0,
		CorrelationID: 7,
		ClientId:      "kafka-cli",
		TopicArray:    []request.Topic{{TopicName: "foo"}},
	}
	var buf bytes.Buffer

	// Construct the binary-encoded message client side
	binary.Write(&buf, binary.BigEndian, uint32(32))                    // Message Length
	binary.Write(&buf, binary.BigEndian, uint16(TestReq.ApiKey))        // API Key
	binary.Write(&buf, binary.BigEndian, uint16(TestReq.ApiVersion))    // API Version
	binary.Write(&buf, binary.BigEndian, uint32(TestReq.CorrelationID)) // Correlation ID

	clientID := TestReq.ClientId
	binary.Write(&buf, binary.BigEndian, uint16(len(clientID))) // Client ID Length
	buf.WriteString(clientID)                                   // Client ID

	binary.Write(&buf, binary.BigEndian, uint8(0))                         // Empty Tagged Field Array
	binary.Write(&buf, binary.BigEndian, uint8(len(TestReq.TopicArray)+1)) // Topics Array Length

	// Add topics
	for _, topic := range TestReq.TopicArray {
		binary.Write(&buf, binary.BigEndian, uint8(len(topic.TopicName)+1)) // Topic Name Length
		buf.WriteString(topic.TopicName)                                    // Topic Name
		binary.Write(&buf, binary.BigEndian, uint8(0))                      // Empty TAG_BUFFER
	}

	binary.Write(&buf, binary.BigEndian, uint32(100)) // Response Partition Limit
	binary.Write(&buf, binary.BigEndian, uint8(0xff)) // Cursor: Null
	binary.Write(&buf, binary.BigEndian, uint8(0))    // Empty Tagged Field Array

	return buf
}

func ConstructDesidredMessage() bytes.Buffer {
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

	return desiredResponse
}

func TestHandleConn(t *testing.T) {

	testRequestMessage := ConstructEncodedMessage()

	req, err := request.Deserialize(&testRequestMessage)
	if err != nil {
		t.Errorf("Failed to get Request")
	}

	desiredResponse := ConstructDesidredMessage()

	res, err := response.Serialize(req)
	if err != nil {
		t.FailNow()
		return
	}

	if !bytes.Equal(res, desiredResponse.Bytes()) {
		log.Println("res and desiredResponse not same")
		t.FailNow()
	}
}
