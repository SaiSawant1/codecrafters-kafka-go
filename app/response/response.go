package response

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/request"
	"github.com/gofrs/uuid"
)

type ApiVersion struct {
	ApiKey int
	Min    int
	Max    int
}

func GetErrrorResponse(req request.Request) []byte {
	var messageBody bytes.Buffer
	binary.Write(&messageBody, binary.BigEndian, req.CorrelationID)
	binary.Write(&messageBody, binary.BigEndian, uint16(35))

	var errorMessage bytes.Buffer

	binary.Write(&errorMessage, binary.BigEndian, uint32(messageBody.Len()))
	binary.Write(&errorMessage, binary.BigEndian, messageBody.Bytes())
	return errorMessage.Bytes()
}

func Serialize(req request.Request) ([]byte, error) {
	switch req.ApiVersion {
	case 0:
		res, err := SerializeVersion0(req)
		if err != nil {
			return nil, err
		}
		return res, nil

	case 4:
		res, err := SerializeVersion4(req)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	return []byte{}, nil
}

func SerializeVersion0(req request.Request) ([]byte, error) {

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

	fmt.Println(response.Bytes())

	return response.Bytes(), nil
}
func SerializeVersion4(req request.Request) ([]byte, error) {
	var responseHeader bytes.Buffer

	// CorrelationID
	binary.Write(&responseHeader, binary.BigEndian, &req.CorrelationID)

	var responseBody bytes.Buffer
	// Body
	binary.Write(&responseBody, binary.BigEndian, uint16(0))

	apiVersionArr := []ApiVersion{ApiVersion{ApiKey: 1, Min: 0, Max: 17}, ApiVersion{ApiKey: 18, Min: 0, Max: 4}, ApiVersion{ApiKey: 75, Min: 0, Max: 0}}

	binary.Write(&responseBody, binary.BigEndian, uint8(len(apiVersionArr)+1))

	for _, apiVersion := range apiVersionArr {

		binary.Write(&responseBody, binary.BigEndian, uint16(apiVersion.ApiKey))
		binary.Write(&responseBody, binary.BigEndian, uint16(apiVersion.Min))
		binary.Write(&responseBody, binary.BigEndian, uint16(apiVersion.Max))

		binary.Write(&responseBody, binary.BigEndian, uint8(0x00))

	}

	binary.Write(&responseBody, binary.BigEndian, uint32(0))
	binary.Write(&responseBody, binary.BigEndian, uint8(0x00))

	messageLength := responseHeader.Len() + responseBody.Len()

	var response bytes.Buffer

	binary.Write(&response, binary.BigEndian, uint32(messageLength))
	binary.Write(&response, binary.BigEndian, responseHeader.Bytes())
	binary.Write(&response, binary.BigEndian, responseBody.Bytes())
	return response.Bytes(), nil
}

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

	return response.Bytes(), nil

}
