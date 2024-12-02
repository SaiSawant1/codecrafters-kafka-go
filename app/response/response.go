package response

import (
	"bytes"
	"encoding/binary"
	"log"

	"github.com/codecrafters-io/kafka-starter-go/app/request"
	"github.com/gofrs/uuid"
)

func CreateResponse(req request.Request) ([]byte, error) {

	var response bytes.Buffer

	var header bytes.Buffer

	// Header
	if err := binary.Write(&header, binary.BigEndian, req.CorrelationID); err != nil {
		return []byte{}, err
	}
	if err := binary.Write(&header, binary.BigEndian, uint8(0)); err != nil {
		return []byte{}, err
	}

	// Body
	var body bytes.Buffer

	// Throttle Time
	if err := binary.Write(&body, binary.BigEndian, uint32(0)); err != nil {
		return []byte{}, err
	}
	// Topic Length
	if err := binary.Write(&body, binary.BigEndian, uint8(len(req.TopicArray)+1)); err != nil {
		return []byte{}, err
	}
	// Encode Topic
	for i := 0; i < len(req.TopicArray); i++ {
		// Error Code
		if err := binary.Write(&body, binary.BigEndian, uint16(3)); err != nil {
			return []byte{}, err
		}
		// Topic
		// Topic Length 1 byte
		topicNameLength := len(req.TopicArray[i].TopicName)
		topicName := req.TopicArray[i].TopicName
		if err := binary.Write(&body, binary.BigEndian, uint8(topicNameLength+1)); err != nil {
			return []byte{}, err
		}
		body.WriteString(string(topicName))

		nullUUID := uuid.UUID{}
		if err := binary.Write(&body, binary.BigEndian, nullUUID.Bytes()); err != nil {
			return []byte{}, err
		}
		if err := binary.Write(&body, binary.BigEndian, int8(0x00)); err != nil {
			return []byte{}, err
		}
		if err := binary.Write(&body, binary.BigEndian, int8(0x01)); err != nil {

			return []byte{}, err
		}
		if err := binary.Write(&body, binary.BigEndian, uint32(0x00000df8)); err != nil {
			return []byte{}, err
		}

		if err := binary.Write(&body, binary.BigEndian, uint8(0)); err != nil {
			return []byte{}, err
		}
	}
	if err := binary.Write(&body, binary.BigEndian, uint8(0xff)); err != nil {
		return []byte{}, err
	}
	if err := binary.Write(&body, binary.BigEndian, uint8(0)); err != nil {
		return []byte{}, err
	}

	messageLength := len(header.Bytes()) + len(body.Bytes())

	binary.Write(&response, binary.BigEndian, uint32(messageLength))
	binary.Write(&response, binary.BigEndian, header.Bytes())
	binary.Write(&response, binary.BigEndian, body.Bytes())

	log.Println(response.Bytes())
	return response.Bytes(), nil

}
