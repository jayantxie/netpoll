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

//go:build !race
// +build !race

package netpoll

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

type ObjectBuffer struct {
	length int
	head   *objectBufferNode // pointed node has not been released
	read   *objectBufferNode // pointed node has not been read
	write  *objectBufferNode // pointed node has not been written
	flush  *objectBufferNode // pointed node has not been flushed
}

type objectBufferNode struct {
	pointer unsafe.Pointer
	next    *objectBufferNode
}

func NewObjectBuffer() *ObjectBuffer {
	node := newObjectBufferNode()
	return &ObjectBuffer{
		head:  node,
		read:  node,
		write: node,
		flush: node,
	}
}

func newObjectBufferNode() *objectBufferNode {
	node := objectPool.Get().(*objectBufferNode)
	return node
}

var objectPool = sync.Pool{
	New: func() interface{} {
		return &objectBufferNode{}
	},
}

func (b *ObjectBuffer) GetSlice(p []unsafe.Pointer) []unsafe.Pointer {
	if b.length == 0 {
		return nil
	}
	node, flush := b.read, b.flush
	var i int
	for i = 0; node != flush && i < len(p); node = node.next {
		p[i] = node.pointer
		i++
	}
	return p[:i]
}

func (b *ObjectBuffer) Skip(n int) error {
	if b.length < n {
		return fmt.Errorf("object buffer skip[%d] not enough", n)
	}
	b.length -= n
	for i := 0; i < n; i++ {
		b.read = b.read.next
	}
	return nil
}

func (b *ObjectBuffer) Release() error {
	for b.head != b.read {
		node := b.head
		b.head = b.head.next
		node.Release()
	}
	return nil
}

func (b *ObjectBuffer) Read() (p unsafe.Pointer, err error) {
	if b.length == 0 {
		return nil, errors.New("object buffer read not enough")
	}
	b.length--
	p = b.read.pointer
	b.read = b.read.next
	return
}

func (b *ObjectBuffer) Len() int {
	return b.length
}

// ------------------------------------------ implement writer -------------------------------------------

func (b *ObjectBuffer) Write(pointer unsafe.Pointer) error {
	b.write.pointer = pointer
	b.write.next = newObjectBufferNode()
	b.write = b.write.next
	return nil
}

func (b *ObjectBuffer) Flush() error {
	var n int
	for node := b.flush; node != b.write; node = node.next {
		n++
	}
	b.flush = b.write
	b.length += n
	return nil
}

func (b *ObjectBuffer) Close() (err error) {
	b.length = 0
	for node := b.head; node != nil; {
		nd := node
		node = node.next
		nd.Release()
	}
	return nil
}

// ------------------------------------------ object buffer node ------------------------------------------

func (n *objectBufferNode) Release() {
	n.pointer = nil
	n.next = nil
	objectPool.Put(n)
}
