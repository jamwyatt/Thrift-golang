// Thrift and golang experiment
package main

/*
Thrift and golang experiment
Copyright (C) 2015 J. Robert Wyatt

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
*/

/*

 */

import (
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/jamwyatt/ThriftTest/gen-go/ops"
	"time"
)

type OpsHandler struct {
	workQueue chan ops.Work
}

func NewOpsHandler(workQueue chan ops.Work) *OpsHandler {
	return &OpsHandler{workQueue}
}

func (p *OpsHandler) SendWorkAsync(w *ops.Work) (err error) {
	p.workQueue <- *w
	return nil
}

func (p *OpsHandler) SendWorkSync(w *ops.Work) (r *ops.Result_, err error) {
	p.workQueue <- *w
	return &ops.Result_{}, nil
}

func runServer(addr string, workQueue chan ops.Work) error {
	transport, err := thrift.NewTServerSocket(addr)
	if err != nil {
		return err
	}
	fmt.Printf("%T\n", transport)

	transportFactory := thrift.NewTTransportFactory()
	if transportFactory == nil {
		fmt.Println("Failed to create new TransportFactory")
		return nil
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	handler := NewOpsHandler(workQueue)
	processor := ops.NewProducerProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	fmt.Println("Starting the simple server... on ", addr)
	return server.Serve()
}

func main() {

	workQueue := make(chan ops.Work, 10)
	go runServer("localhost:5000", workQueue)

	m := make(map[int32]int)
	var count int64
	start := time.Now()
	for {
		select {
		case msg := <-workQueue:
			count += 1
			m[msg.Priority] += 1
			if count%1000 == 0 {
				stop := time.Now()
				fmt.Println("messages .... ", count, stop.Sub(start), m)
				start = time.Now()
			}
		}

	}

}
