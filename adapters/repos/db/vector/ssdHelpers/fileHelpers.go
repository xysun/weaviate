package ssdhelpers

import (
	"encoding/binary"
	"os"
	"reflect"
	"unsafe"
)

func DumpGraphToDiskWithBinary(path string, edges [][]uint64, r int) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	for _, edge := range edges {
		data := make([]byte, r*8)
		bytesFromUint64s(edge, data)
		f.Write(data)
	}
}

func DumpGraphToDisk(path string, edges [][]uint64, r int) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	for _, edge := range edges {
		p := uintptr(unsafe.Pointer(&edge))
		var data []byte
		size := len(edge)
		sh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
		sh.Data = p
		sh.Len = size * 8
		sh.Cap = size * 8

		f.Write(data)
		if size == r {
			continue
		}
		data = make([]byte, (r-size)*8)
		f.Write(data)
	}
}

func ReadGraphRowWithBinary(f *os.File, x uint64, r int) []uint64 {
	buf := make([]uint64, r)
	data := make([]byte, r*8)
	f.Seek(int64(r)*int64(8)*int64(x), 0)
	f.Read(data)
	uint64sFromBytes(data, buf)
	return buf
}

func uint64FromBytes(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes)
}

func uint64sFromBytes(data []byte, results []uint64) {
	for i := range results {
		results[i] = uint64FromBytes(data[i*8 : i*8+8])
	}
}

func bytesFromUint64(source uint64, bytes []byte) {
	binary.LittleEndian.PutUint64(bytes, source)
}

func bytesFromUint64s(results []uint64, data []byte) {
	for i := range results {
		bytesFromUint64(results[i], data[i*8:i*8+8])
	}
}

func ReadGraphRow(f *os.File, x uint64, r int) []uint64 {
	buf := make([]uint64, r)
	p := uintptr(unsafe.Pointer(&buf))
	var data []byte

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	sh.Data = p
	sh.Len = r
	sh.Cap = r

	f.Seek(int64(r)*int64(8)*int64(x), 0)
	f.Read(data)
	data = make([]byte, 1)
	f.Read(data)
	sh.Data = 0x0
	sh.Len = 0
	sh.Cap = 0
	return buf
}
