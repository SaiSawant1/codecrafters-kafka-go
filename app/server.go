package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var TAG_BUFFER = []byte{0x00}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	startServer()

}

func startServer() {

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	for {
		req, err := NewRequestFromConn(conn)

		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(req)

		res := bytes.NewBuffer([]byte{})

		binary.Write(res, binary.BigEndian, uint32(req.CorrelationID))

		var errorCode = uint16(0)

		if int16(req.ApiVersion) < 0 || int16(req.ApiVersion) > 4 {
			errorCode = uint16(35)
		}

		binary.Write(res, binary.BigEndian, uint16(errorCode))
		binary.Write(res, binary.BigEndian, byte(2))
		binary.Write(res, binary.BigEndian, uint16(2))
		binary.Write(res, binary.BigEndian, uint16(18))
		binary.Write(res, binary.BigEndian, uint16(3))
		binary.Write(res, binary.BigEndian, uint16(4))

		res.Write(TAG_BUFFER)

		binary.Write(res, binary.BigEndian, res.Len())
		io.Copy(conn, res)

	}

}

type Request struct {
	ApiKey        uint16
	ApiVersion    uint16
	CorrelationID uint32
	ClientId      string
	TaggedFields  string
}

func NewRequestFromConn(conn net.Conn) (Request, error) {
	var size uint32

	err := binary.Read(conn, binary.BigEndian, &size)
	if err != nil {
		return Request{}, err
	}

	var req = Request{}

	binary.Read(conn, binary.BigEndian, &req.ApiKey)
	binary.Read(conn, binary.BigEndian, &req.ApiVersion)
	binary.Read(conn, binary.BigEndian, &req.CorrelationID)

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	conn.Read(make([]byte, 1024))

	return req, nil
}
