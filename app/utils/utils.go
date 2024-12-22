package utils

import (
	"bytes"
	"encoding/binary"
)

const Describe_TOPIC_PARTITIONS = 0
const API_VERSION = 4
const FETCH = 16

func ReadUINT8(dataField *uint8, data *bytes.Buffer) error {
	if err := binary.Read(data, binary.BigEndian, dataField); err != nil {
		return err
	}

	return nil
}

func ReadUINT16(dataField *uint16, data *bytes.Buffer) error {
	if err := binary.Read(data, binary.BigEndian, dataField); err != nil {
		return err
	}
	return nil
}

func ReadUINT32(dataField *uint32, data *bytes.Buffer) error {
	if err := binary.Read(data, binary.BigEndian, dataField); err != nil {
		return err
	}
	return nil
}

func ReadUINT64(dataField *uint64, data *bytes.Buffer) error {
	if err := binary.Read(data, binary.BigEndian, dataField); err != nil {
		return err
	}
	return nil
}
