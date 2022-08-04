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

/*
#cgo LDFLAGS: -lrpal -L/data04/VMs/xzy-kitex/xzy-data/rpal-lib/build
#cgo CFLAGS: -I/data04/VMs/xzy-kitex/xzy-data/rpal-lib/
#include <rpal.h>
#include <sys/mman.h>
*/
import "C"

import (
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
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

	// rpal
	rpalReadTimer        *time.Timer
	rpalReadTrigger      chan struct{}
	rpalWaitReadSize     int32
	rpalWrittenTrigger   chan error
	inputObjects         *ObjectBuffer
	outputObjects        *ObjectBuffer
	inputObjectsBarrier  *rpalBarrier
	outputObjectsBarrier *rpalBarrier
	senderRtp            *C.rpal_thread_pool_t
}

// ------------------------------------------ rpal ------------------------------------------

func (c *connection) WriteObjects(pointers []unsafe.Pointer) error {
	for i := range pointers {
		if err := c.outputObjects.Write(pointers[i]); err != nil {
			return err
		}
	}
	return nil
	// outputObjects:
	// 1. Write
}

func (c *connection) ReadObjects(n int) (ptrs []unsafe.Pointer, err error) {
	// inputObjects:
	// 1. waitRead
	// 2. Read
	if err = c.rpalWaitRead(n); err != nil {
		return nil, err
	}
	ptrs = make([]unsafe.Pointer, 0, n)
	for i := 0; i < n; i++ {
		var ptr unsafe.Pointer
		ptr, err = c.inputObjects.Read()
		if err != nil {
			return
		}
		ptrs = append(ptrs, ptr)
	}
	return ptrs, nil
}

func (c *connection) RpalRelease() (err error) {
	// inputObjects:
	// 1. Release
	// 2. notify client
	err = c.inputObjects.Release()
	if err != nil {
		return Exception(err, "when rpal release")
	}
	return rpalCallAck(c.senderRtp, c.sfd)
}

func (c *connection) RpalFlush() error {
	// outputObjects:
	// 2. Flush
	// 3. GetSlice
	// 4. rpal call
	// 5. Skip
	// 6. Release
	if !c.IsActive() || !c.lock(flushing) {
		return Exception(ErrConnClosed, "when flush")
	}
	defer c.unlock(flushing)
	c.outputObjects.Flush()
	return c.rpalflush()
}

func (c *connection) rpalWaitRead(n int) (err error) {
	if n <= c.inputObjects.Len() {
		return nil
	}
	atomic.StoreInt32(&c.rpalWaitReadSize, int32(n))
	defer atomic.StoreInt32(&c.rpalWaitReadSize, 0)
	if c.readTimeout > 0 {
		return c.rpalWaitReadWithTimeout(n)
	}
	// wait full n
	for c.inputObjects.Len() < n {
		if c.IsActive() {
			<-c.rpalReadTrigger
			continue
		}
		return Exception(ErrConnClosed, "rpal wait read")
	}
	return nil
}

func (c *connection) rpalWaitReadWithTimeout(n int) (err error) {
	// set read timeout
	if c.rpalReadTimer == nil {
		c.rpalReadTimer = time.NewTimer(c.readTimeout)
	} else {
		c.rpalReadTimer.Reset(c.readTimeout)
	}
	for c.inputObjects.Len() < n {
		if !c.IsActive() {
			err = Exception(ErrConnClosed, "rpal wait read")
			break
		}
		select {
		case <-c.rpalReadTimer.C:
			if c.inputBuffer.Len() >= n {
				return nil
			}
			return Exception(ErrReadTimeout, c.remoteAddr.String())
		case <-c.rpalReadTrigger:
			continue
		}
	}

	// clean timer.C
	if !c.rpalReadTimer.Stop() {
		<-c.rpalReadTimer.C
	}
	return err
}

func (c *connection) rpalTriggerRead() {
	select {
	case c.rpalReadTrigger <- struct{}{}:
	default:
	}
}

func (c *connection) rpalTriggerWritten(err error) {
	select {
	case c.rpalWrittenTrigger <- err:
	default:
	}
}

// ------------------------------------------ private ------------------------------------------

var barrierPool = sync.Pool{
	New: func() interface{} {
		return &barrier{
			bs:  make([][]byte, barriercap),
			ivs: make([]syscall.Iovec, barriercap),
		}
	},
}

var rpalBarrierPool = sync.Pool{
	New: func() interface{} {
		return &rpalBarrier{
			bs: make([]unsafe.Pointer, rpalbarriercap),
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

	// init rpal buffer, barrier, finalizer
	c.rpalReadTrigger = make(chan struct{}, 1)
	c.rpalWrittenTrigger = make(chan error, 1)
	c.inputObjects, c.outputObjects = NewObjectBuffer(), NewObjectBuffer()
	c.inputObjectsBarrier, c.outputObjectsBarrier = rpalBarrierPool.Get().(*rpalBarrier), rpalBarrierPool.Get().(*rpalBarrier)

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
	op.RpalInputs, op.RpalInputAck = c.rpalInputs, c.rpalInputAck
	op.RpalOutputAck = c.rpalOutputAck

	// if connection has been registered, must reuse poll here.
	if c.pd != nil && c.pd.operator != nil {
		op.poll = c.pd.operator.poll
	}
	c.operator = op
}

// onHup means close by poller.
func (c *connection) onHup(p Poll) error {
	if c.closeBy(poller) {
		c.triggerRead()
		c.triggerWrite(ErrConnClosed)
		c.rpalTriggerRead()
		c.rpalTriggerWritten(ErrConnClosed)
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
		c.rpalTriggerRead()
		c.rpalTriggerWritten(ErrConnClosed)
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

	c.inputObjects.Close()
	rpalBarrierPool.Put(c.inputObjectsBarrier)

	c.outputObjects.Close()
	rpalBarrierPool.Put(c.outputObjectsBarrier)
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
			return c.inputObjects.Len() > 0 || c.Reader().Len() > 0
		},
		func(c *connection) {
			_ = onRequest(c.ctx, c)
		},
	)
	// if not processed, should trigger read
	return !processed
}

func (c *connection) rpalInputs() (rs []unsafe.Pointer) {
	return c.inputObjectsBarrier.bs
}

func (c *connection) rpalInputAck(n int) (err error) {
	if n <= 0 {
		return nil
	}
	for i := 0; i < n; i++ {
		c.inputObjects.Write(c.inputObjectsBarrier.bs[i])
		c.inputObjectsBarrier.bs[i] = nil
	}
	c.inputObjects.Flush()

	length := c.inputObjects.Len()
	needTrigger := true
	if length == n { // first start onRequest
		needTrigger = c.onRequest()
	}
	if needTrigger && length >= int(atomic.LoadInt32(&c.rpalWaitReadSize)) {
		c.rpalTriggerRead()
	}
	return nil
}

func (c *connection) rpalOutputAck() (err error) {
	c.rpalTriggerWritten(nil)
	return nil
}

func (c *connection) rpalflush() (err error) {
	objs := c.outputObjects.GetSlice(c.outputObjectsBarrier.bs)
	err = c.rpalsendmsg(c.fd, c.sfd, objs)
	if err != nil {
		return Exception(err, "when rpal flush")
	}
	err = c.outputObjects.Skip(len(objs))
	if err != nil {
		return Exception(err, "when rpal flush")
	}
	err = c.outputObjects.Release()
	if err != nil {
		return Exception(err, "when rpal flush")
	}
	return
}

func (c *connection) rpalsendmsg(fd, sfd int, objs []unsafe.Pointer) (err error) {
	if len(objs) == 0 {
		return nil
	}
	err = rpalSendMsg(c.senderRtp, sfd, objs)
	if err != nil {
		return err
	}
	// TODO: what if connection close while server is still copying?
	err = <-c.rpalWrittenTrigger
	return err
}
