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

	request_api_version := binary.BigEndian.Uint16(buff[6:8])

	if int16(request_api_version) > 4 || int16(request_api_version) < 0 {

		resp := make([]byte, 16)
		copy(resp, []byte{0, 0, 0, 0})
		copy(resp[4:8], buff[8:12])
		copy(resp[8:10], []byte{0, 35})

		conn.Write(resp)
		return
	}
	resp := make([]byte, 8)
	copy(resp, []byte{0, 0, 0, 0})
	copy(resp[4:], buff[8:12])
	conn.Write(resp)
}
