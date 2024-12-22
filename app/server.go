package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	metadata "github.com/codecrafters-io/kafka-starter-go/app/meta-data"
	"github.com/codecrafters-io/kafka-starter-go/app/request"
	"github.com/codecrafters-io/kafka-starter-go/app/response"
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
	metadata.SetClusterTopics()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		conn.SetDeadline(time.Now().Add(10 * time.Second))
		go handleConn(&conn)
	}

}

func handleConn(conn *net.Conn) {
	for {
		buf := make([]byte, 1024)
		n, err := (*conn).Read(buf)
		if n == 0 {
			continue
		}
		if err != nil {
			(*conn).Close()
			return
		}

		reqData := bytes.NewBuffer(buf[:n])
		req, err := request.Deserialize(reqData)
		if err != nil {
			if err.Error() == errors.ErrUnsupported.Error() {
				errorRes := response.GetErrorResponse(req)
				(*conn).Write(errorRes)
				continue
			}
			if err == io.EOF {
				continue
			}
		}

		res, err := response.Serialize(req)
		if err != nil {
			log.Fatalf("Failed to create response from request %s\n", err.Error())
			(*conn).Close()
			return
		}

		_, err = (*conn).Write(res)
		if err != nil {
			log.Fatalln("Failed to write to client")
			(*conn).Close()
			return
		}
	}
}
