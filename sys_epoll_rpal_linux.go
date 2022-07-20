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
#include <stdint.h>
*/
import "C"

import (
	"errors"
	"syscall"
	"unsafe"
)

const EPOLLET = -syscall.EPOLLET

type epollevent struct {
	events uint32
	data   [8]byte // unaligned uintptr
}

// EpollCtl implements epoll_ctl.
func EpollCtl(epfd int, op int, fd int, event *epollevent) (err error) {
	// fmt.Println("RpalEpollCtl tid: ", unix.Gettid())
	ret := C.rpal_epoll_ctl((*C.rpal_thread_pool_t)(unsafe.Pointer(recverRtp)),
		C.int(epfd), C.int(op), C.int(fd), (*C.struct_epoll_event)(unsafe.Pointer(event)))
	if ret != 0 {
		return errors.New("epoll ctl error")
	}
	return nil
}

// EpollWait implements epoll_wait.
func EpollWait(fd int, events []epollevent, msec int) (int, error) {
	ret := C.rpal_epoll_wait(C.int(fd), (*C.struct_epoll_event)(unsafe.Pointer(&events[0])), C.int(len(events)), C.int(msec))
	return int(ret), nil
}
