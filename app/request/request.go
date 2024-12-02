package request

import (
	"bytes"
	"encoding/binary"
	"log"
)

type Topic struct {
	TopicName string
}

type Request struct {
	ApiKey        uint16
	ApiVersion    uint16
	CorrelationID uint32
	ClientId      string

	TopicArray []Topic
}

func NewRequestFromData(data bytes.Buffer) (Request, error) {
	log.Print("Parsing the the req")
	var req = Request{}

	var size uint32

	// Read size of the message (4 bytes 32 bit unsigned int)
	if err := binary.Read(&data, binary.BigEndian, &size); err != nil {
		return Request{}, err
	}

	// Read ApiKey of the message (2 bytes 16 bit unsigned int)
	if err := binary.Read(&data, binary.BigEndian, &req.ApiKey); err != nil {
		return Request{}, err
	}

	// Read ApiVersion of the message (2 bytes 16 bit unsigned int)
	if err := binary.Read(&data, binary.BigEndian, &req.ApiVersion); err != nil {
		return Request{}, err
	}

	// Read CorrelationID of the message (4 bytes 32 bit unsigned int)
	if err := binary.Read(&data, binary.BigEndian, &req.CorrelationID); err != nil {
		return Request{}, err
	}

	/*
			   Client Id is two parts [2 bytes indicate the length of the client id].
		      Using the length we can the client id
	*/
	var clientIdLength uint16
	if err := binary.Read(&data, binary.BigEndian, &clientIdLength); err != nil {
		return Request{}, err
	}
	var clientId = make([]byte, clientIdLength)
	if err := binary.Read(&data, binary.BigEndian, &clientId); err != nil {
		return Request{}, err
	}
	req.ClientId = string(clientId)

	// Skip the tag buffer
	var tag byte
	if err := binary.Read(&data, binary.BigEndian, &tag); err != nil {
		return Request{}, err
	}

	//TODO: read the body of the request which container topic array

	var topicArrayLength byte
	if err := binary.Read(&data, binary.BigEndian, &topicArrayLength); err != nil {
		return Request{}, err
	}

	for i := 0; i < int(topicArrayLength)-1; i++ {
		var topic Topic
		var topicNameLength byte

		if err := binary.Read(&data, binary.BigEndian, &topicNameLength); err != nil {
			return Request{}, err
		}
		var topicName = make([]byte, topicNameLength-1)
		if err := binary.Read(&data, binary.BigEndian, &topicName); err != nil {
			return Request{}, err
		}
		topic.TopicName = string(topicName)

		req.TopicArray = append(req.TopicArray, topic)
		var tag byte
		if err := binary.Read(&data, binary.BigEndian, &tag); err != nil {
			return Request{}, err
		}

	}

	return req, nil
}
