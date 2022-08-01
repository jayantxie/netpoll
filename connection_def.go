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

//go:build !rpal
// +build !rpal

package netpoll

import (
	"sync"
	"syscall"
	"time"
)

// connection is the implement of Connection
type connection struct {
	netFD
	onEvent
	locker
	operator        *FDOperator
	readTimeout     time.Duration
	readTimer       *time.Timer
	readTrigger     chan struct{}
	waitReadSize    int32
	writeTrigger    chan error
	inputBuffer     *LinkBuffer
	outputBuffer    *LinkBuffer
	inputBarrier    *barrier
	outputBarrier   *barrier
	supportZeroCopy bool
	maxSize         int // The maximum size of data between two Release().
	bookSize        int // The size of data that can be read at once.
}

var barrierPool = sync.Pool{
	New: func() interface{} {
		return &barrier{
			bs:  make([][]byte, barriercap),
			ivs: make([]syscall.Iovec, barriercap),
		}
	},
}

// init initialize the connection with options
func (c *connection) init(conn Conn, opts *options) (err error) {
	// init buffer, barrier, finalizer
	c.readTrigger = make(chan struct{}, 1)
	c.writeTrigger = make(chan error, 1)
	c.bookSize, c.maxSize = block1k/2, pagesize
	c.inputBuffer, c.outputBuffer = NewLinkBuffer(pagesize), NewLinkBuffer()
	c.inputBarrier, c.outputBarrier = barrierPool.Get().(*barrier), barrierPool.Get().(*barrier)

	c.initNetFD(conn) // conn must be *netFD{}
	c.initFDOperator()
	c.initFinalizer()

	syscall.SetNonblock(c.fd, true)
	// enable TCP_NODELAY by default
	switch c.network {
	case "tcp", "tcp4", "tcp6":
		setTCPNoDelay(c.fd, true)
	}
	// check zero-copy
	if setZeroCopy(c.fd) == nil && setBlockZeroCopySend(c.fd, defaultZeroCopyTimeoutSec, 0) == nil {
		c.supportZeroCopy = true
	}

	// connection initialized and prepare options
	return c.onPrepare(opts)
}

func (c *connection) initFDOperator() {
	op := allocop()
	op.FD = c.fd
	op.OnRead, op.OnWrite, op.OnHup = nil, nil, c.onHup
	op.Inputs, op.InputAck = c.inputs, c.inputAck
	op.Outputs, op.OutputAck = c.outputs, c.outputAck

	// if connection has been registered, must reuse poll here.
	if c.pd != nil && c.pd.operator != nil {
		op.poll = c.pd.operator.poll
	}
	c.operator = op
}

// onRequest is responsible for executing the closeCallbacks after the connection has been closed.
func (c *connection) onRequest() (needTrigger bool) {
	onRequest, ok := c.onRequestCallback.Load().(OnRequest)
	if !ok {
		return true
	}
	processed := c.onProcess(
		// only process when conn active and have unread data
		func(c *connection) bool {
			return c.Reader().Len() > 0
		},
		func(c *connection) {
			_ = onRequest(c.ctx, c)
		},
	)
	// if not processed, should trigger read
	return !processed
}

// onHup means close by poller.
func (c *connection) onHup(p Poll) error {
	if c.closeBy(poller) {
		c.triggerRead()
		c.triggerWrite(ErrConnClosed)
		// It depends on closing by user if OnConnect and OnRequest is nil, otherwise it needs to be released actively.
		// It can be confirmed that the OnRequest goroutine has been exited before closecallback executing,
		// and it is safe to close the buffer at this time.
		onConnect, _ := c.onConnectCallback.Load().(OnConnect)
		onRequest, _ := c.onRequestCallback.Load().(OnRequest)
		if onConnect != nil || onRequest != nil {
			c.closeCallback(true)
		}
	}
	return nil
}

// onClose means close by user.
func (c *connection) onClose() error {
	if c.closeBy(user) {
		c.triggerRead()
		c.triggerWrite(ErrConnClosed)
		c.closeCallback(true)
		return nil
	}
	if c.isCloseBy(poller) {
		// Connection with OnRequest of nil
		// relies on the user to actively close the connection to recycle resources.
		c.closeCallback(true)
	}
	return nil
}

// closeBuffer recycle input & output LinkBuffer.
func (c *connection) closeBuffer() {
	c.inputBuffer.Close()
	barrierPool.Put(c.inputBarrier)

	c.outputBuffer.Close()
	barrierPool.Put(c.outputBarrier)
}
