// Copyright 2021 CloudWeGo Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build rpal
// +build rpal

package netpoll

import (
	"context"
	"net"
	"time"
)

func NewRpalDialer() Dialer {
	return &rpalDialer{}
}

type rpalDialer struct{}

func (d *rpalDialer) DialTimeout(network, address string, timeout time.Duration) (conn net.Conn, err error) {
	conn, err = d.DialConnection(network, address, timeout)
	return conn, err
}

func (d *rpalDialer) DialConnection(network, address string, timeout time.Duration) (connection Connection, err error) {
	ctx := context.Background()
	if timeout > 0 {
		subCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		ctx = subCtx
	}

	switch network {
	// rpal only support uds now
	case "unix", "unixgram", "unixpacket":
		var raddr *UnixAddr
		raddr, err = ResolveUnixAddr(network, address)
		if err != nil {
			return
		}
		connection, err = DialUnix(network, nil, raddr)
		if err != nil {
			return
		}
	default:
		return nil, net.UnknownNetworkError(network)
	}
	return connection, ClientRpalHandshake(connection, 0)
}
