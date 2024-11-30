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
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn)
	}

}

func handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		req, err := NewReqFromConn(conn)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(req)
		res := bytes.NewBuffer([]byte{})
		binary.Write(res, binary.BigEndian, uint32(req.CorrelationID))
		var errorCode = uint16(0)
		if int(req.ApiVersion) < 0 || int(req.ApiVersion) > 4 {
			errorCode = 35
		}
		binary.Write(res, binary.BigEndian, uint16(errorCode))
		if req.ApiKey == 18 {
			binary.Write(res, binary.BigEndian, byte(3))
			binary.Write(res, binary.BigEndian, uint16(18))
			binary.Write(res, binary.BigEndian, uint16(3))
			binary.Write(res, binary.BigEndian, uint16(4))
			res.Write(TAG_BUFFER)
			binary.Write(res, binary.BigEndian, uint16(75))
			binary.Write(res, binary.BigEndian, uint16(0))
			binary.Write(res, binary.BigEndian, uint16(0))
			res.Write(TAG_BUFFER)
			binary.Write(res, binary.BigEndian, uint32(0))
			res.Write(TAG_BUFFER)
		}
		binary.Write(conn, binary.BigEndian, uint32(res.Len()))
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

func NewReqFromConn(conn net.Conn) (Request, error) {
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
func (r *Request) Reader() (uint32, io.Reader) {
	var buff bytes.Buffer
	binary.Write(&buff, binary.BigEndian, r.ApiKey)
	binary.Write(&buff, binary.BigEndian, r.ApiVersion)
	binary.Write(&buff, binary.BigEndian, r.CorrelationID)
	return uint32(buff.Len()), &buff
}
