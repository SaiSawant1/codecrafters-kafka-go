package response

import (
	"bytes"
	"encoding/binary"
	"fmt"

	metadata "github.com/codecrafters-io/kafka-starter-go/app/meta-data"
	"github.com/codecrafters-io/kafka-starter-go/app/request"
	"github.com/codecrafters-io/kafka-starter-go/app/utils"
	"github.com/gofrs/uuid"
)

type ApiVersion struct {
	ApiKey int
	Min    int
	Max    int
}

func Serialize(req request.Request) ([]byte, error) {
	switch req.ApiVersion {
	case utils.Describe_TOPIC_PARTITIONS:
		res, err := SerializeVersion0(req)
		if err != nil {
			return nil, err
		}
		return res, nil

	case utils.API_VERSION:
		res, err := SerializeVersion4(req)
		if err != nil {
			return nil, err
		}
		return res, nil
	case utils.FETCH:
		res, err := SerializeVersion16(req)
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
	// Topic Array Length
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
		_, _ = body.Write([]byte(topicName))

		if clusterTopic.ErrorCode == 3 {

			nullUUID := uuid.UUID{}
			if err := binary.Write(&body, binary.BigEndian, nullUUID.Bytes()); err != nil {
				return []byte{}, err
			}

		} else {
			if err := binary.Write(&body, binary.BigEndian, clusterTopic.TopicId.Bytes()); err != nil {
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

			for i := 0; i < partionsLength; i++ {
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
				binary.Write(&body, binary.BigEndian, uint8(0x00))
			}
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

	binary.Write(&responseBody, binary.BigEndian, uint32(0x00000000))
	binary.Write(&responseBody, binary.BigEndian, uint8(0x00))

	messageLength := responseHeader.Len() + responseBody.Len()

	var response bytes.Buffer

	binary.Write(&response, binary.BigEndian, uint32(messageLength))
	binary.Write(&response, binary.BigEndian, responseHeader.Bytes())
	binary.Write(&response, binary.BigEndian, responseBody.Bytes())
	return response.Bytes(), nil
}

func SerializeVersion16(req request.Request) ([]byte, error) {
	var responseBody bytes.Buffer

	// Write throttle_time_ms (4 bytes, int32)
	if err := binary.Write(&responseBody, binary.BigEndian, uint32(0)); err != nil {
		return nil, fmt.Errorf("failed to write throttle_time_ms: %v", err)
	}

	// Write error_code (2 bytes, int16)
	if err := binary.Write(&responseBody, binary.BigEndian, uint16(0)); err != nil {
		return nil, fmt.Errorf("failed to write error_code: %v", err)
	}

	// Write session_id (4 bytes, int32)
	if err := binary.Write(&responseBody, binary.BigEndian, uint32(0)); err != nil {
		return nil, fmt.Errorf("failed to write session_id: %v", err)
	}

	// Write responses field (0 elements for empty topics array)
	// Array length is 0, so we only write the length (4 bytes, int32)
	if err := binary.Write(&responseBody, binary.BigEndian, uint16(0)); err != nil {
		return nil, fmt.Errorf("failed to write responses field: %v", err)
	}

	var responseHeader bytes.Buffer

	// Write correlation_id (4 bytes, int32)
	if err := binary.Write(&responseHeader, binary.BigEndian, req.CorrelationID); err != nil {
		return nil, fmt.Errorf("failed to write correlation_id: %v", err)
	}

	// Write TAG_BUFFER field (1 byte, uint8)
	// Assuming this is a reserved field, set to 0 for now
	if err := binary.Write(&responseHeader, binary.BigEndian, uint8(0)); err != nil {
		return nil, fmt.Errorf("failed to write TAG_BUFFER: %v", err)
	}

	var response bytes.Buffer

	// Write total message length (4 bytes, int32)
	totalLength := responseHeader.Len() + responseBody.Len()
	if err := binary.Write(&response, binary.BigEndian, int32(totalLength)); err != nil {
		return nil, fmt.Errorf("failed to write message length: %v", err)
	}

	// Write response header
	if _, err := response.Write(responseHeader.Bytes()); err != nil {
		return nil, fmt.Errorf("failed to write response header: %v", err)
	}

	// Write response body
	if _, err := response.Write(responseBody.Bytes()); err != nil {
		return nil, fmt.Errorf("failed to write response body: %v", err)
	}

	return response.Bytes(), nil
}

func GetErrorResponse(req request.Request) []byte {
	var messageBody bytes.Buffer
	binary.Write(&messageBody, binary.BigEndian, req.CorrelationID)
	binary.Write(&messageBody, binary.BigEndian, uint16(35))

	var errorMessage bytes.Buffer

	binary.Write(&errorMessage, binary.BigEndian, uint32(messageBody.Len()))
	binary.Write(&errorMessage, binary.BigEndian, messageBody.Bytes())
	return errorMessage.Bytes()
}
