package request

import (
	"bytes"
	"encoding/binary"
	"log"
	"testing"
)

func TestNewReqFromConn(t *testing.T) {
	TestReq := Request{
		ApiKey:        75,
		ApiVersion:    0,
		CorrelationID: 7,
		ClientId:      "kafka-cli",
		TopicArray:    []Topic{{TopicName: "foo"}},
	}

	log.Println("Starting TestNewReqFromConn...")

	var buf bytes.Buffer

	// Construct the binary-encoded message
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

	req, err := NewRequestFromData(buf)
	if err != nil {
		t.Errorf("Failed to get Request")
	}

	if req.ApiKey != TestReq.ApiKey || req.ApiVersion != TestReq.ApiVersion ||
		req.CorrelationID != TestReq.CorrelationID || req.ClientId != TestReq.ClientId {
		t.Errorf("Request does not match expected values.")
	}

	for i, topic := range req.TopicArray {
		if topic.TopicName != TestReq.TopicArray[i].TopicName {
			t.Errorf("Topic %d does not match expected value.", i)
		}
	}

	log.Println("Test completed.")
}
