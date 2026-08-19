package main

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
)

type RequestType uint8

const (
	RequestTypeRead RequestType = iota
	RequestTypeWrite
)

func main() {
	address := "0.0.0.0:5555"
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("WARN: Listening on %s\n", address)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
		}
		go serve(conn)
	}
}

func serve(conn net.Conn) {
	for {
		var requestType RequestType
		err := binary.Read(conn, binary.BigEndian, &requestType)
		if err != nil {
			_, _ = conn.Write([]byte("ERROR: invalid request type"))
			_ = conn.Close()
			return
		}
		var fileNameSize uint16
		err = binary.Read(conn, binary.BigEndian, &fileNameSize)
		if err != nil {
			_, _ = conn.Write([]byte("ERROR: invalid filesize"))
			_ = conn.Close()
			return
		}
		filename := make([]byte, fileNameSize)
		_, err = io.ReadFull(conn, filename)
		if err != nil {
			_, _ = conn.Write([]byte("ERROR: invalid filesize"))
			_ = conn.Close()
			return
		}

		switch requestType {
		case RequestTypeRead:
			err = sendFileContent(conn, string(filename))
			if err != nil {
				_, _ = conn.Write([]byte(err.Error()))
				_ = conn.Close()
				return
			}
		case RequestTypeWrite:
			err = writeFileContent(conn, string(filename))
			if err != nil {
				_, _ = conn.Write([]byte(err.Error()))
				_ = conn.Close()
				return
			}
		default:
			_, _ = conn.Write([]byte("invalid request type"))
			_ = conn.Close()
			return
		}
	}
}

func sendFileContent(conn net.Conn, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Println(err)
		}
	}()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	filesize := stat.Size()

	err = binary.Write(conn, binary.BigEndian, uint64(filesize))
	if err != nil {
		return err
	}
	_, err = io.CopyN(conn, f, int64(filesize))
	if err != nil {
		return err
	}
	return nil
}

func writeFileContent(conn net.Conn, filename string) error {
	var filesize uint64
	err := binary.Read(conn, binary.BigEndian, &filesize)
	if err != nil {
		return err
	}
	dirname := filepath.Dir(filename)
	err = os.MkdirAll(dirname, 0o755)
	if err != nil {
		return err
	}
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Println(err)
		}
	}()

	_, err = io.CopyN(f, conn, int64(filesize))
	if err != nil {
		return err
	}
	return nil
}
