package ssdhelpers

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"reflect"
	"unsafe"
)

func DumpGraphToDiskWithBinary(path string, edges [][]uint64, r int, vectorForIDThunk VectorForID, dimensions int) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	vectorSize := dimensions * 4
	for i, edge := range edges {
		v, _ := vectorForIDThunk(context.Background(), uint64(i))
		data := make([]byte, r*8+vectorSize)
		bytesFromFloat32s(v, data)
		bytesFromUint64s(edge, data[vectorSize:])
		f.Write(data)
	}
}

func WriteRowToGraphWithBinary(f *os.File, position uint64, r int, dimensions int, vector []float32, outNeighbors []uint64) {
	vectorSize := dimensions * 4
	f.Seek((int64(r*8+vectorSize))*int64(position), 0)
	data := make([]byte, r*8+vectorSize)
	bytesFromFloat32s(vector, data)
	bytesFromUint64s(outNeighbors, data[vectorSize:])
	f.Write(data)
}

func WriteOutNeighborsToGraphWithBinary(f *os.File, position uint64, r int, dimensions int, outNeighbors []uint64) {
	vectorSize := dimensions * 4
	f.Seek((int64(r*8+vectorSize))*int64(position)+int64(vectorSize), 0)
	data := make([]byte, r*8)
	bytesFromUint64s(outNeighbors, data)
	f.Write(data)
}

func DumpGraphToDisk(path string, edges [][]uint64, r int, vectorForIDThunk VectorForID) {
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

func ReadGraphRowWithBinary(f *os.File, x uint64, r int, dimensions int) ([]uint64, []float32) {
	buf := make([]uint64, r)
	vector := make([]float32, dimensions)
	vectorSize := dimensions * 4
	data := make([]byte, r*8+vectorSize)
	f.Seek((int64(r*8+vectorSize))*int64(x), 0)
	f.Read(data)
	float32sFromBytes(data, vector)
	uint64sFromBytes(data[vectorSize:], buf)
	return buf, vector
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

func bytesFromFloat32(source float32, bytes []byte) {
	bits := math.Float32bits(source)
	binary.LittleEndian.PutUint32(bytes, bits)
}

func float32FromBytes(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	float := math.Float32frombits(bits)
	return float
}

func bytesFromUint64s(results []uint64, data []byte) {
	for i := range results {
		bytesFromUint64(results[i], data[i*8:i*8+8])
	}
}

func float32sFromBytes(data []byte, results []float32) {
	for i := range results {
		results[i] = float32FromBytes(data[i*4 : i*4+4])
	}
}

func bytesFromFloat32s(source []float32, data []byte) {
	for i := range source {
		bytesFromFloat32(source[i], data[i*4:i*4+4])
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
