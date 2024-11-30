package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092", err.Error())
		os.Exit(1)
	}
	defer l.Close()

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	buff := make([]byte, 1024)
	_, err = conn.Read(buff)
	if err != nil {
		fmt.Println("Failed to read from connection", err.Error())
		os.Exit(1)
	}

	length := []byte{0, 0, 0, 19}

	api_version := buff[6:8]

	correlation_id := buff[8:12]

	var version_error []byte

	switch binary.BigEndian.Uint16(api_version) {

	case 0, 1, 2, 3, 4:
		version_error = []byte{0, 0}
	default:
		version_error = []byte{0, 35}

	}

	res := append(length, correlation_id...)
	res = append(res, version_error...)
	_, err = conn.Write(res)
	if err != nil {
		fmt.Println("Failed to read from connection", err.Error())
		os.Exit(1)
	}

	ret_key := []byte{2, 0, 18, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0}
	conn.Write(ret_key)
}
