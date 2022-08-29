package netpoll

import (
	"testing"
	"unsafe"
)

type MockA struct {
	Str string
}

type MockB struct {
	Value int
}

func TestObjectBuffer(t *testing.T) {
	buffer := NewObjectBuffer()

	a := &MockA{Str: "mock a"}
	var err error

	err = buffer.Write(unsafe.Pointer(a))
	MustNil(t, err)

	b := &MockB{Value: 123}

	err = buffer.Write(unsafe.Pointer(b))
	MustNil(t, err)

	// try to read without flush
	_, err = buffer.Read()
	Assert(t, err != nil)

	Assert(t, buffer.Len() == 0)

	err = buffer.Flush()
	MustNil(t, err)

	Assert(t, buffer.Len() == 2)

	p, err := buffer.Read()
	MustNil(t, err)
	Assert(t, (*MockA)(p) == a)

	Assert(t, buffer.Len() == 1)

	// release the first node
	err = buffer.Release(false)
	MustNil(t, err)

	c := &MockA{Str: "mock c"}
	err = buffer.Write(unsafe.Pointer(c))
	MustNil(t, err)

	d := &MockB{Value: 456}
	err = buffer.Write(unsafe.Pointer(d))
	MustNil(t, err)

	Assert(t, buffer.Len() == 1)

	err = buffer.Skip(3)
	Assert(t, err != nil)

	err = buffer.Skip(1)
	MustNil(t, err)

	Assert(t, buffer.Len() == 0)

	err = buffer.Flush()
	MustNil(t, err)

	Assert(t, buffer.Len() == 2)

	ps := make([]unsafe.Pointer, 32)
	objs := buffer.GetSlice(ps)
	Assert(t, len(objs) == 2)

	Assert(t, (*MockA)(objs[0]) == c)
	Assert(t, (*MockB)(objs[1]) == d)

	Assert(t, buffer.Len() == 2)

	err = buffer.Skip(2)
	MustNil(t, err)

	Assert(t, buffer.Len() == 0)

	// release the rest three nodes
	err = buffer.Release(false)
	MustNil(t, err)
}
