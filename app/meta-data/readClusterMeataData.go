package metadata

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"

	"github.com/gofrs/uuid"
)

const Feature_LEVEL_RECORD int = 12
const TOPIC_RECORD int = 2
const PARTISION_RECORD int = 3

type ClusterTopicPartition struct {
	ErrorCode                             int16 // 0 indicates NO_ERROR
	PartitionIndex                        int32
	LeaderID                              int32
	LeaderEpoch                           int32
	ReplicaNodeIDs                        []int32
	InsyncReplicaNodeIDs                  []int32
	EligibleLeaderReplicaNodeIDs          []int32
	LastKnownEligibleLeaderReplicaNodeIDs []int32
	OfflineReplicaNodeIDs                 []int32
}

type ClusterTopic struct {
	ErrorCode  int16
	TopicId    uuid.UUID
	Partitions []ClusterTopicPartition
}

var ClusterTopics map[string]*ClusterTopic = map[string]*ClusterTopic{}

func SetClusterTopics() error {
	path := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	file, err := os.ReadFile(path)
	if err != nil {
		log.Println("Failed to Read Log File.")
		return err
	}
	reader := bytes.NewBuffer(file)

	partitions := []struct {
		TopicID uuid.UUID
		ClusterTopicPartition
	}{}

	for {
		_ = reader.Next(8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4)

		if reader.Len() == 0 {
			break
		}

		var recordLength int32

		_ = binary.Read(reader, binary.BigEndian, &recordLength)

		for range recordLength {
			size, _ := binary.ReadVarint(reader)
			recordBuf := bytes.NewBuffer(reader.Next(int(size)))
			_ = recordBuf.Next(1)
			_, _ = binary.ReadVarint(recordBuf)
			_, _ = binary.ReadVarint(recordBuf)
			keyLength, _ := binary.ReadVarint(recordBuf)

			if keyLength > 0 {
				_ = recordBuf.Next(int(keyLength))
			}

			valueLength, _ := binary.ReadVarint(recordBuf)
			valueBuf := bytes.NewBuffer(recordBuf.Next(int(valueLength)))

			_ = valueBuf.Next(1)

			var recordType int8
			_ = binary.Read(valueBuf, binary.BigEndian, &recordType)
			_ = valueBuf.Next(1)

			switch recordType {

			case int8(TOPIC_RECORD):
				topicNameLegth, _ := binary.ReadUvarint(valueBuf)
				topicName, topicId := make([]byte, topicNameLegth-1), make([]byte, 16)
				_ = binary.Read(valueBuf, binary.BigEndian, &topicName)
				_ = binary.Read(valueBuf, binary.BigEndian, &topicId)
				id, _ := uuid.FromBytes(topicId)

				ClusterTopics[string(topicName)] = &ClusterTopic{TopicId: id}

			case int8(PARTISION_RECORD):

				var partition ClusterTopicPartition
				_ = binary.Read(valueBuf, binary.BigEndian, &partition.PartitionIndex)

				topicId := make([]byte, 16)

				_ = binary.Read(valueBuf, binary.BigEndian, &topicId)
				id, _ := uuid.FromBytes(topicId)

				replicaNodeIDsLenth, _ := binary.ReadUvarint(valueBuf)
				partition.ReplicaNodeIDs = make([]int32, replicaNodeIDsLenth)
				for i := range replicaNodeIDsLenth {
					_ = binary.Read(valueBuf, binary.BigEndian, &partition.ReplicaNodeIDs[i])
				}

				insyncReplicaNodeIDsLenth, _ := binary.ReadUvarint(valueBuf)
				partition.InsyncReplicaNodeIDs = make([]int32, insyncReplicaNodeIDsLenth)
				for i := range insyncReplicaNodeIDsLenth {
					_ = binary.Read(valueBuf, binary.BigEndian, &partition.InsyncReplicaNodeIDs[i])
				}

				_, _ = binary.ReadUvarint(valueBuf)
				_, _ = binary.ReadUvarint(valueBuf)

				_ = binary.Read(valueBuf, binary.BigEndian, &partition.LeaderID)
				_ = binary.Read(valueBuf, binary.BigEndian, &partition.LeaderEpoch)

				partitions = append(partitions, struct {
					TopicID uuid.UUID
					ClusterTopicPartition
				}{

					TopicID: id, ClusterTopicPartition: partition,
				})

			}
		}
	}
setPartition:
	for i := range partitions {
		for topicName := range ClusterTopics {
			if ClusterTopics[topicName].TopicId == partitions[i].TopicID {
				ClusterTopics[topicName].Partitions = append(ClusterTopics[topicName].Partitions, partitions[i].ClusterTopicPartition)
				continue setPartition
			}
		}

	}

	return nil

}

func GetClusterTopic(topic string) *ClusterTopic {
	clusterTopic, ok := ClusterTopics[topic]
	if ok {
		return clusterTopic
	}

	return &ClusterTopic{ErrorCode: 3}
}
