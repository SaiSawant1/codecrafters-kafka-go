package response

import (
	"bytes"
	"encoding/binary"

	metadata "github.com/codecrafters-io/kafka-starter-go/app/meta-data"
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
	if err := binary.Write(&header, binary.BigEndian, uint8(0x00)); err != nil {
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

		topicNameLength := len(req.TopicArray[i].TopicName)
		topicName := req.TopicArray[i].TopicName

		clusterTopic := metadata.GetClusterTopic(topicName)

		if clusterTopic.ErrorCode == 3 {
			if err := binary.Write(&body, binary.BigEndian, uint16(0x0003)); err != nil {
				return []byte{}, err
			}
		} else {
			if err := binary.Write(&body, binary.BigEndian, uint16(0x0000)); err != nil {
				return []byte{}, err
			}
		}

		if err := binary.Write(&body, binary.BigEndian, uint8(topicNameLength+1)); err != nil {
			return []byte{}, err
		}
		body.WriteString(string(topicName))

		if clusterTopic.ErrorCode == 3 {

			nullUUID := uuid.UUID{}
			if err := binary.Write(&body, binary.BigEndian, nullUUID.Bytes()); err != nil {
				return []byte{}, err
			}

		} else {
			if err := binary.Write(&body, binary.BigEndian, clusterTopic.TopicId); err != nil {
				return []byte{}, err
			}
		}
		// Is internal
		if err := binary.Write(&body, binary.BigEndian, uint8(0x00)); err != nil {
			return []byte{}, err
		}

		// Partition array
		partionsLength := len(clusterTopic.Partitions)
		if partionsLength == 0 {
			if err := binary.Write(&body, binary.BigEndian, uint8(0x01)); err != nil {
				return []byte{}, err
			}
		} else {

			if err := binary.Write(&body, binary.BigEndian, uint8(partionsLength+1)); err != nil {
				return []byte{}, err
			}

			for i := range partionsLength {
				partition := clusterTopic.Partitions[i]
				binary.Write(&body, binary.BigEndian, uint16(0x0000))
				binary.Write(&body, binary.BigEndian, uint32(partition.PartitionIndex))
				binary.Write(&body, binary.BigEndian, uint32(partition.LeaderID))
				binary.Write(&body, binary.BigEndian, uint32(partition.LeaderEpoch))

				replicaArrayLength := len(partition.ReplicaNodeIDs)
				binary.Write(&body, binary.BigEndian, uint8(replicaArrayLength+1))
				for _, replicaNode := range partition.ReplicaNodeIDs {
					binary.Write(&body, binary.BigEndian, replicaNode)
				}

				inSyncReplicaLen := len(partition.InsyncReplicaNodeIDs)
				binary.Write(&body, binary.BigEndian, uint8(inSyncReplicaLen+1))
				for _, inSyncReplica := range partition.InsyncReplicaNodeIDs {
					binary.Write(&body, binary.BigEndian, inSyncReplica)
				}

				eligibleLeaderReplicaNodeIDs := len(partition.EligibleLeaderReplicaNodeIDs)
				binary.Write(&body, binary.BigEndian, uint8(eligibleLeaderReplicaNodeIDs+1))
				for _, eligibleLeaderReplicaNodeID := range partition.EligibleLeaderReplicaNodeIDs {
					binary.Write(&body, binary.BigEndian, eligibleLeaderReplicaNodeID)
				}

				lastKnownEligibleLeaderReplicaNodeIDs := len(partition.LastKnownEligibleLeaderReplicaNodeIDs)
				binary.Write(&body, binary.BigEndian, uint8(lastKnownEligibleLeaderReplicaNodeIDs+1))
				for _, lastKnownEligibleLeaderReplicaNodeID := range partition.LastKnownEligibleLeaderReplicaNodeIDs {
					binary.Write(&body, binary.BigEndian, lastKnownEligibleLeaderReplicaNodeID)
				}

				offlineReplicaNodes := len(partition.OfflineReplicaNodeIDs)
				binary.Write(&body, binary.BigEndian, uint8(offlineReplicaNodes+1))
				for _, offlineReplicaNode := range partition.OfflineReplicaNodeIDs {
					binary.Write(&body, binary.BigEndian, offlineReplicaNode)
				}
			}
			binary.Write(&body, binary.BigEndian, uint8(0x00))
		}

		// Topic Authorized
		if err := binary.Write(&body, binary.BigEndian, uint32(0x00000df8)); err != nil {
			return []byte{}, err
		}
		// Tag buffer
		if err := binary.Write(&body, binary.BigEndian, uint8(0x00)); err != nil {
			return []byte{}, err
		}
	}

	if err := binary.Write(&body, binary.BigEndian, uint8(0xff)); err != nil {
		return []byte{}, err
	}
	if err := binary.Write(&body, binary.BigEndian, uint8(0x00)); err != nil {
		return []byte{}, err
	}

	messageLength := len(header.Bytes()) + len(body.Bytes())

	binary.Write(&response, binary.BigEndian, uint32(messageLength))
	binary.Write(&response, binary.BigEndian, header.Bytes())
	binary.Write(&response, binary.BigEndian, body.Bytes())

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
