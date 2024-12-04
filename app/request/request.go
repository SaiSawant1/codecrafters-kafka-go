package request

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"

	"github.com/codecrafters-io/kafka-starter-go/app/utils"
)

type Topic struct {
	TopicName string
}

type Request struct {
	ApiKey                uint16
	ApiVersion            uint16
	CorrelationID         uint32
	ClientId              string
	ClientSoftwareVersion string

	TopicArray []Topic
}

func (r *Request) ParseRequestHeader(data *bytes.Buffer) error {
	messageLength := make([]byte, 4)
	if err := binary.Read(data, binary.BigEndian, messageLength); err != nil {
		return err
	}

	if err := utils.ReadUINT16(&r.ApiKey, data); err != nil {
		return err
	}

	if err := utils.ReadUINT16(&r.ApiVersion, data); err != nil {
		return err
	}
	if err := utils.ReadUINT32(&r.CorrelationID, data); err != nil {
		return err
	}

	if err := r.ReadClientId(data); err != nil {
		return err
	}
	return nil
}

func (r *Request) ParseRequestBody(data *bytes.Buffer) error {

	switch r.ApiVersion {
	case 0:
		if err := r.DecodeVersion0(data); err != nil {
			return err
		}
	case 4:
		if err := r.DecodeVersion4(data); err != nil {
			return err
		}
	}

	return nil

}

func (r *Request) DecodeVersion0(data *bytes.Buffer) error {
	var topicArrayLength uint8

	if err := utils.ReadUINT8(&topicArrayLength, data); err != nil {
		return nil
	}

	for i := 0; i < int(topicArrayLength)-1; i++ {
		var topic Topic
		var topicNameLength byte

		if err := binary.Read(data, binary.BigEndian, &topicNameLength); err != nil {
			return err
		}
		var topicName = make([]byte, topicNameLength-1)
		if err := binary.Read(data, binary.BigEndian, &topicName); err != nil {
			return err
		}
		topic.TopicName = string(topicName)
		r.TopicArray = append(r.TopicArray, topic)
		r.SkipTagBuffer(data)
	}

	return nil

}

func (r *Request) DecodeVersion4(data *bytes.Buffer) error {

	var clientIdLength uint8
	if err := utils.ReadUINT8(&clientIdLength, data); err != nil {
		fmt.Println("While Reading the client id length")
		return err
	}

	clientId := make([]byte, int(clientIdLength)-1)

	if err := binary.Read(data, binary.BigEndian, &clientId); err != nil {
		fmt.Println("While Reading the client id", err.Error())
		return err
	}

	var versionLength uint8
	if err := utils.ReadUINT8(&versionLength, data); err != nil {
		fmt.Print("While Reading the version")
		return err
	}
	version := make([]byte, int(versionLength)-1)

	if err := binary.Read(data, binary.BigEndian, &version); err != nil {
		fmt.Print("While Reading the version")
		return err
	}
	r.ClientSoftwareVersion = string(version)
	r.SkipTagBuffer(data)
	return nil

}

func (r Request) CheckVersionValidity() bool {
	if r.ApiVersion >= 0 && r.ApiVersion <= 4 {
		return true
	}
	return false
}

func (r *Request) ReadClientId(data *bytes.Buffer) error {
	var clientIdLenght uint16
	if err := utils.ReadUINT16(&clientIdLenght, data); err != nil {
		return err
	}
	clientId := make([]byte, clientIdLenght)

	if err := binary.Read(data, binary.NativeEndian, clientId); err != nil {
		return err
	}
	r.ClientId = string(clientId)
	r.SkipTagBuffer(data)
	return nil

}

func (r Request) SkipTagBuffer(data *bytes.Buffer) error {
	var tagBuffer byte
	if err := binary.Read(data, binary.BigEndian, &tagBuffer); err != nil {
		return err
	}
	return nil
}

func Deserialize(data *bytes.Buffer) (Request, error) {
	newRequest := Request{}

	if err := newRequest.ParseRequestHeader(data); err != nil {
		log.Println(err.Error())
		return Request{}, err
	}
	if newRequest.CheckVersionValidity() == false {
		return newRequest, errors.New(errors.ErrUnsupported.Error())
	}
	if err := newRequest.ParseRequestBody(data); err != nil {
		return Request{}, err
	}
	fmt.Println("New Request", newRequest)
	return newRequest, nil
}

func NewRequestFromData(data *bytes.Buffer) (Request, error) {

	var req = Request{}

	var size uint32

	// Read size of the message (4 bytes 32 bit unsigned int)
	if err := binary.Read(data, binary.BigEndian, &size); err != nil {
		return Request{}, err
	}

	// Read ApiKey of the message (2 bytes 16 bit unsigned int)
	if err := binary.Read(data, binary.BigEndian, &req.ApiKey); err != nil {
		return Request{}, err
	}

	// Read ApiVersion of the message (2 bytes 16 bit unsigned int)
	if err := binary.Read(data, binary.BigEndian, &req.ApiVersion); err != nil {
		return Request{}, err
	}

	// Read CorrelationID of the message (4 bytes 32 bit unsigned int)
	if err := binary.Read(data, binary.BigEndian, &req.CorrelationID); err != nil {
		return Request{}, err
	}

	if req.ApiVersion != 0 {
		err := errors.New(errors.ErrUnsupported.Error())
		return req, err
	}

	/*
			   Client Id is two parts [2 bytes indicate the length of the client id].
		      Using the length we can the client id
	*/
	var clientIdLength uint16
	if err := binary.Read(data, binary.BigEndian, &clientIdLength); err != nil {
		return Request{}, err
	}
	var clientId = make([]byte, clientIdLength)
	if err := binary.Read(data, binary.BigEndian, &clientId); err != nil {
		return Request{}, err
	}
	req.ClientId = string(clientId)

	// Skip the tag buffer
	var tag byte
	if err := binary.Read(data, binary.BigEndian, &tag); err != nil {
		return Request{}, err
	}

	var topicArrayLength byte
	if err := binary.Read(data, binary.BigEndian, &topicArrayLength); err != nil {
		return Request{}, err
	}

	for i := 0; i < int(topicArrayLength)-1; i++ {
		var topic Topic
		var topicNameLength byte

		if err := binary.Read(data, binary.BigEndian, &topicNameLength); err != nil {
			return Request{}, err
		}
		var topicName = make([]byte, topicNameLength-1)
		if err := binary.Read(data, binary.BigEndian, &topicName); err != nil {
			return Request{}, err
		}
		topic.TopicName = string(topicName)

		req.TopicArray = append(req.TopicArray, topic)
		var tag byte
		if err := binary.Read(data, binary.BigEndian, &tag); err != nil {
			return Request{}, err
		}

	}
	return req, nil
}
