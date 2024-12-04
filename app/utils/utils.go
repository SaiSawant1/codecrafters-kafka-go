package utils

import (
	"bytes"
	"encoding/binary"
)

func ReadUINT8(reqField *uint8, data *bytes.Buffer) error {
	if err := binary.Read(data, binary.BigEndian, reqField); err != nil {
		return err
	}

	return nil
}
func ReadUINT16(reqField *uint16, data *bytes.Buffer) error {
	if err := binary.Read(data, binary.BigEndian, reqField); err != nil {
		return err
	}
	return nil
}
func ReadUINT32(reqField *uint32, data *bytes.Buffer) error {
	if err := binary.Read(data, binary.BigEndian, reqField); err != nil {
		return err
	}
	return nil
}
