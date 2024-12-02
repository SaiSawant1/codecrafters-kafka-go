package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"

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
	for {
		var reqData bytes.Buffer
		n, err := io.Copy(&reqData, conn)
		if err != nil {
			conn.Close()
			return
		} else if n == 0 {
			continue
		}
		req, err := request.NewRequestFromData(reqData)
		if err != nil {
			log.Fatalln("Failed to parse request")
			conn.Close()
			return
		}

		res, err := response.CreateResponse(req)
		if err != nil {
			log.Fatalln("Failed to create response from request")
			conn.Close()
			return
		}

		_, err = conn.Write(res)
		if err != nil {
			log.Fatalln("Failed to write to client")
			conn.Close()
			return
		}
	}
}
