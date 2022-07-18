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
#include <stdio.h>
#include <stdint.h>
*/
import "C"

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// #define EPOLLIN		(__force __poll_t)0x00000001
	// #define EPOLLPRI	(__force __poll_t)0x00000002
	// #define EPOLLOUT	(__force __poll_t)0x00000004
	// #define EPOLLERR	(__force __poll_t)0x00000008
	// #define EPOLLHUP	(__force __poll_t)0x00000010
	// #define EPOLLNVAL	(__force __poll_t)0x00000020
	// #define EPOLLRDNORM	(__force __poll_t)0x00000040
	// #define EPOLLRDBAND	(__force __poll_t)0x00000080
	// #define EPOLLWRNORM	(__force __poll_t)0x00000100
	// #define EPOLLWRBAND	(__force __poll_t)0x00000200
	// #define EPOLLMSG	(__force __poll_t)0x00000400
	// #define EPOLLRDHUP	(__force __poll_t)0x00002000
	EPOLLRPALIN  uint32 = 0x00020000
	EPOLLRPALACK uint32 = 0x00040000
)

const rpalbarriercap = 32

type rpalBarrier struct {
	bs []unsafe.Pointer
}

var (
	recverRtp *C.rpal_thread_pool_t
	tcount    int32 = -1
)

type RPALERR uintptr

const (
	ERR_REQ_PENDING = RPALERR(C.TASK_RPAL_PENDING)
)

func (e RPALERR) Error() string {
	return "rpal error"
}

var (
	defaultRpalHandshakeTimeout = 2 * time.Second
	RpalClientPreface           = []byte("rpal_hi")
)

func ClientRpalHandshake(conn Connection, timeout time.Duration) (err error) {
	if timeout < defaultRpalHandshakeTimeout {
		timeout = defaultRpalHandshakeTimeout
	}
	errChan := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	runTask(ctx, func() {
		zw, zr, c := conn.Writer(), conn.Reader(), conn.(*connection)
		// write client preface
		_, err = zw.WriteBinary(RpalClientPreface)
		if err != nil {
			errChan <- err
			return
		}
		if err = zw.Flush(); err != nil {
			errChan <- err
			return
		}
		// read server rpal id
		buf, err := zr.ReadBinary(4)
		if err != nil {
			errChan <- err
			return
		}
		serverRpalId := binary.BigEndian.Uint32(buf)
		if senderRtp, status := rpalRequestService(int(serverRpalId)); status != 0 {
			errChan <- fmt.Errorf("error request server rpal service: %d, %d", serverRpalId, status)
			return
		} else {
			c.senderRtp = senderRtp
		}
		// write client rpal id
		buf, err = zw.Malloc(4)
		if err != nil {
			errChan <- err
			return
		}
		binary.BigEndian.PutUint32(buf, uint32(rpalGetId()))
		if err = zw.Flush(); err != nil {
			errChan <- err
			return
		}
		// set sfd
		c.sfd = rpalFdmap(c.fd)
	})
	select {
	case <-ctx.Done():
		cancel()
		return errors.New("rpal handshake timeout")
	case err = <-errChan:
		cancel()
		return err
	}
}

func ServerRpalHandshake(conn Connection, timeout time.Duration) (err error) {
	if timeout < defaultRpalHandshakeTimeout {
		timeout = defaultRpalHandshakeTimeout
	}
	errChan := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	runTask(ctx, func() {
		zw, zr, c := conn.Writer(), conn.Reader(), conn.(*connection)
		// read client preface
		preface, err := zr.ReadBinary(4)
		if err != nil {
			errChan <- err
			return
		}
		if bytes.Equal(preface, RpalClientPreface) {
			errChan <- fmt.Errorf("cannot handshake, preface mismatch! %s", string(preface))
			return
		}
		// write server rpal id
		buf, err := zw.Malloc(4)
		if err != nil {
			errChan <- err
			return
		}
		binary.BigEndian.PutUint32(buf, uint32(rpalGetId()))
		if err = zw.Flush(); err != nil {
			errChan <- err
			return
		}
		// read client rpal id
		buf, err = zr.ReadBinary(4)
		if err != nil {
			errChan <- err
			return
		}
		clientRpalId := binary.BigEndian.Uint32(buf)
		if senderRtp, status := rpalRequestService(int(clientRpalId)); status != 0 {
			errChan <- fmt.Errorf("error request server rpal service: %d, %d", clientRpalId, status)
			return
		} else {
			c.senderRtp = senderRtp
		}
		// set sfd
		c.sfd = rpalFdmap(c.fd)
	})

	select {
	case <-ctx.Done():
		cancel()
		return errors.New("rpal handshake timeout")
	case err = <-errChan:
		cancel()
		return err
	}
}

func rpalThreadPoolCreate(nrThreads int) int {
	recverRtp = C.rpal_thread_pool_create(C.int(nrThreads), 0, 0)
	if recverRtp == nil {
		return -1
	}
	return 0
}

func rpalEnableService() int {
	ret := C.rpal_enable_service(unsafe.Pointer(recverRtp))
	return int(ret)
}

func rpalThreadInitialize() int {
	runtime.LockOSThread()
	thread := atomic.AddInt32(&tcount, 1)
	if thread >= int32(recverRtp.nr_threads) {
		panic("Thread Init Error, thread: ")
	}
	ptr1 := (unsafe.Pointer(uintptr(unsafe.Pointer(recverRtp.rtis)) + uintptr(C.sizeof_rpal_thread_info_t*C.int(thread))))
	ret := C.rpal_thread_initialize_ingo(ptr1)
	return int(ret)
}

func rpalGetId() int {
	return int(C.rpal_get_id())
}

func rpalRequestService(id int) (*C.rpal_thread_pool_t, int) {
	var senderRtp C.rpal_thread_pool_t
	retVal := C.rpal_request_service(C.int(id), unsafe.Pointer(&senderRtp))
	return &senderRtp, int(retVal)
}

func rpalFdmap(cfd int) int {
	sfd := C.rpal_uds_fdmap(C.int(cfd))
	return int(sfd)
}

func rpalSendMsg(senderRtp *C.rpal_thread_pool_t, sfd int, objs []unsafe.Pointer) (err error) {
	status := int(C.rpal_write_msg(runtime.RpalTkey, senderRtp,
		C.int(sfd),
		(*C.uintptr_t)(unsafe.Pointer(&objs[0])),
		C.int(len(objs)),
	))
	if status != 0 {
		return fmt.Errorf("error send rpal msg, sfd: %d, status: %d", sfd, status)
	}
	return nil
}

func rpalReadMsg(fd int, bs []unsafe.Pointer) (n int, err error) {
	n = int(C.rpal_read_msg(C.int(fd), (*C.uintptr_t)(unsafe.Pointer(&bs[0])), C.int(len(bs))))
	if n < 0 {
		return 0, fmt.Errorf("error read rpal msg, fd: %d, status: %d", fd, n)
	}
	return
}

func rpalCallAck(senderRtp *C.rpal_thread_pool_t, sfd int) (err error) {
	status := C.rpal_call_ack(runtime.RpalTkey, senderRtp, C.int(sfd))
	if int(status) != 0 {
		return fmt.Errorf("error send call ack, sfd: %d, status: %d", sfd, status)
	}
	return nil
}
