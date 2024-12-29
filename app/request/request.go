package request

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/codecrafters-io/kafka-starter-go/app/utils"
	"github.com/gofrs/uuid"
)

type Describe_Topic_Partition_Request struct {
	TopicArray []string
}
type Api_Version_Request struct {
	ClientID              string
	clientSoftwareVersion string
}

type Fetch_Request_Partition struct {
	PartitionID        int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
}
type Fetch_Request_Topic struct {
	TopicID    uuid.UUID
	Partitions []Fetch_Request_Partition
}
type Fetch_Request struct {
	SessionID int32
	Topics    []Fetch_Request_Topic
}

type Request struct {
	ApiKey        uint16
	ApiVersion    uint16
	CorrelationID uint32
	ClientId      string

	DescribeTopicPartitionRequest *Describe_Topic_Partition_Request
	ApiVersionRequest             *Api_Version_Request
	FetchRequest                  *Fetch_Request
}

func Deserialize(data *bytes.Buffer) (Request, error) {
	newRequest := Request{}

	if err := newRequest.ParseRequestHeader(data); err != nil {
		return Request{}, err
	}
	if newRequest.ApiVersion != utils.FETCH {
		if newRequest.CheckVersionValidity() == false {
			return newRequest, errors.New(errors.ErrUnsupported.Error())
		}
	}
	if err := newRequest.ParseRequestBody(data); err != nil {
		return Request{}, err
	}
	return newRequest, nil
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

func (r Request) CheckVersionValidity() bool {
	if r.ApiVersion >= 0 && r.ApiVersion <= 4 {
		return true
	}
	return false
}

func (r *Request) ParseRequestBody(data *bytes.Buffer) error {

	switch r.ApiVersion {
	case utils.DESCRIBE_TOPIC_PARTITIONS:
		if err := r.DecodeVersion0(data); err != nil {
			return err
		}
	case utils.API_VERSION:
		if err := r.DecodeVersion4(data); err != nil {
			return err
		}
	case utils.FETCH:
		if err := r.DecodeVersion16(data); err != nil {
			return err
		}
	}

	return nil

}

func (r *Request) DecodeVersion0(data *bytes.Buffer) error {
	r.DescribeTopicPartitionRequest = &Describe_Topic_Partition_Request{}
	var topicArrayLength uint8

	if err := utils.ReadUINT8(&topicArrayLength, data); err != nil {
		return nil
	}
	var topicArray []string

	for i := 0; i < int(topicArrayLength)-1; i++ {
		var topicNameLength byte

		if err := binary.Read(data, binary.BigEndian, &topicNameLength); err != nil {
			return err
		}
		var topicName = make([]byte, topicNameLength-1)
		if err := binary.Read(data, binary.BigEndian, &topicName); err != nil {
			return err
		}
		topic := string(topicName)
		topicArray = append(topicArray, topic)
		r.SkipTagBuffer(data)
	}

	r.DescribeTopicPartitionRequest.TopicArray = topicArray

	return nil

}

func (r *Request) DecodeVersion4(data *bytes.Buffer) error {
	r.ApiVersionRequest = &Api_Version_Request{}
	var clientIdLength uint8
	if err := utils.ReadUINT8(&clientIdLength, data); err != nil {
		return err
	}

	clientId := make([]byte, int(clientIdLength)-1)

	if err := binary.Read(data, binary.BigEndian, &clientId); err != nil {
		return err
	}
	r.ApiVersionRequest.ClientID = string(clientId)

	var versionLength uint8
	if err := utils.ReadUINT8(&versionLength, data); err != nil {
		return err
	}
	version := make([]byte, int(versionLength)-1)

	if err := binary.Read(data, binary.BigEndian, &version); err != nil {
		return err
	}
	r.ApiVersionRequest.clientSoftwareVersion = string(version)

	r.SkipTagBuffer(data)
	return nil

}

func (r *Request) DecodeVersion16(data *bytes.Buffer) error {
	data.Next(4 + 4 + 4 + 1)
	fetchRequest := Fetch_Request{
		Topics: []Fetch_Request_Topic{},
	}
	if err := binary.Read(data, binary.BigEndian, &fetchRequest.SessionID); err != nil {
		return err
	}
	data.Next(4)

	var topicLen byte
	if err := binary.Read(data, binary.BigEndian, &topicLen); err != nil {
		return err
	}
	for i := 0; i < int(topicLen); i++ {
		buffer := make([]byte, 16)
		if err := binary.Read(data, binary.BigEndian, &buffer); err != nil {
			return err
		}
		topicId, err := uuid.FromBytes(buffer)
		if err != nil {
			return err
		}
		fetchRequestTopic := Fetch_Request_Topic{
			TopicID: topicId,
		}
		var paritionLen byte
		if err := binary.Read(data, binary.BigEndian, &paritionLen); err != nil {
			return err
		}
		for j := 0; j < int(paritionLen); j++ {
			fetchRequestPartition := Fetch_Request_Partition{}
			binary.Read(data, binary.BigEndian, &fetchRequestPartition.PartitionID)
			binary.Read(data, binary.BigEndian, &fetchRequestPartition.CurrentLeaderEpoch)
			binary.Read(data, binary.BigEndian, &fetchRequestPartition.FetchOffset)
			binary.Read(data, binary.BigEndian, &fetchRequestPartition.LastFetchedEpoch)
			binary.Read(data, binary.BigEndian, &fetchRequestPartition.LogStartOffset)
			binary.Read(data, binary.BigEndian, &fetchRequestPartition.PartitionMaxBytes)
			fetchRequestTopic.Partitions = append(fetchRequestTopic.Partitions, fetchRequestPartition)
			r.SkipTagBuffer(data)
		}
		fetchRequest.Topics = append(fetchRequest.Topics, fetchRequestTopic)
		r.SkipTagBuffer(data)

	}

	r.FetchRequest = &fetchRequest

	return nil

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
