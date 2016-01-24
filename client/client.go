// Thrift and golang experiment
package main

/*
Copyright (C) 2016 J. Robert Wyatt

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
	"errors"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/jamwyatt/Thrift-golang/gen-go/ops"
	"os"
	"strconv"
	"time"
)

func runClient(addr string) *ops.ProducerClient {
	var transport thrift.TTransport
	var err error
	socket, err := thrift.NewTSocket(addr)
	if err != nil {
		fmt.Println("Error opening socket:", err)
		return nil
	}
	transportFactory := thrift.NewTTransportFactory()
	if transportFactory == nil {
		fmt.Println("Failed to create new TransportFactory")
		return nil
	}
	transport = transportFactory.GetTransport(socket)
	if err := transport.Open(); err != nil {
		fmt.Println("Failed to connect to", addr)
		return nil
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	return ops.NewProducerClientFactory(transport, protocolFactory)
}

func runTest(async bool, addr string, i int) error {
	client := runClient(addr)
	if client == nil {
		fmt.Println("Failed to create client to", addr)
		return errors.New("Failed to connect")
	}

	var count int32
	for {
		count = count + 1
		start := time.Now()
		w := ops.Work{"Hello", int32(i), int32((time.Now().UnixNano()) % 1000)}
		stop := time.Now()
		if count%100 == 0 {
			fmt.Println("Client", i, "Sent", count, "messages", "duration:", stop.Sub(start))
		}
		var err error
		if async {
			err = client.SendWorkAsync(&w)
		} else {
			_, err = client.SendWorkSync(&w)
		}
		if err != nil {
			fmt.Printf("Send failed: %T %v\n", err, err)
			return nil
		}
	}
}

func runChild(async bool, i int) {
	for {
		err := runTest(async, "localhost:5000", i)
		if err != nil {
			time.Sleep(time.Second * 2)
		}
	}
}

func main() {
	async := false
	if len(os.Args) > 1 {
		num, err := strconv.Atoi(os.Args[1])
		if err != nil {
			fmt.Println("Failed to detect first arg as int: ", err)
			os.Exit(-1)
		}
		for i := 0; i < num; i++ {
			go runChild(async, i)
		}
		for {
			time.Sleep(time.Second * 10)
		}
	} else {
		runChild(async, 0)
	}
}
