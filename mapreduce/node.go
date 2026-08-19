package mapreduce

import "net"

type Node struct {
	ID       string
	Listener net.Listener
}
