package mapreduce

import (
	"encoding/binary"
	"io"
	"net"
	"os"
)

type RequestType uint8

const (
	RequestTypeRead RequestType = iota
	RequestTypeWrite
)

type FileServerConnection struct {
	address    string
	connection net.Conn
}

func NewFileServerConnection(address string) (*FileServerConnection, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return &FileServerConnection{
		connection: conn,
		address:    address,
	}, nil
}

func (c *FileServerConnection) GetFileContents(filename string) ([]byte, error) {
	err := binary.Write(c.connection, binary.BigEndian, RequestTypeRead)
	if err != nil {
		return nil, err
	}

	err = binary.Write(c.connection, binary.BigEndian, uint16(len(filename)))
	if err != nil {
		return nil, err
	}
	_, err = c.connection.Write([]byte(filename))
	if err != nil {
		return nil, err
	}
	var filesize uint64
	err = binary.Read(c.connection, binary.BigEndian, &filesize)
	if err != nil {
		return nil, err
	}
	content := make([]byte, filesize)
	_, err = io.ReadFull(c.connection, content)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func (c *FileServerConnection) PutFileContents(f *os.File, destFilename string) error {
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	filesize := stat.Size()
	err = binary.Write(c.connection, binary.BigEndian, RequestTypeWrite)
	if err != nil {
		return err
	}

	err = binary.Write(c.connection, binary.BigEndian, uint16(len(destFilename)))
	if err != nil {
		return err
	}
	_, err = c.connection.Write([]byte(destFilename))
	if err != nil {
		return err
	}
	err = binary.Write(c.connection, binary.BigEndian, uint64(filesize))
	if err != nil {
		return err
	}
	_, err = io.CopyN(c.connection, f, filesize)
	return err
}

func (c *FileServerConnection) Close() error {
	return c.connection.Close()
}
