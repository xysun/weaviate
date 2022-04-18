package search

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type RawResults struct {
	length       int
	sizeCapacity int
	sizeUsed     int
	data         []byte
	offsets      []int
	distances    []float32
}

func PreAllocateRawResults(limit int, sizePerObject int) *RawResults {
	rr := RawResults{
		sizeCapacity: limit * sizePerObject,
		data:         make([]byte, limit*sizePerObject),
		offsets:      make([]int, limit),
		distances:    make([]float32, limit),
	}

	return &rr
}

func (rr RawResults) SizeUsed() int {
	return rr.sizeUsed
}

func (rr RawResults) SizeCapacity() int {
	return rr.sizeCapacity
}

func (rr *RawResults) Add(data []byte) {
	var offset int
	if rr.length == 0 {
		offset = 0
	} else {
		offset = rr.offsets[rr.length-1]
	}
	if rr.sizeCapacity-offset < len(data) {
		panic("growing not supported yet")
	}

	rr.sizeUsed += len(data)
	copy(rr.data[offset:offset+len(data)], data)
	rr.offsets[rr.length] = offset + len(data)
	rr.length += 1
}

func (rr *RawResults) WriteJSON(w io.Writer) error {
	// TODO: errors
	// TODO: 0 length
	_, err := w.Write([]byte(`{"objects":[`))
	if err != nil {
		return err
	}

	for i := 0; i < rr.length; i++ {
		var offset int
		if i == 0 {
			offset = 0
		} else {
			offset = int(rr.offsets[i-1])
			w.Write([]byte(","))
		}

		props, err := extractPropsBytes(rr.data[offset:])
		if err != nil {
			return err
		}
		_, err = w.Write(props)
		if err != nil {
			return err
		}

	}
	_, err = w.Write([]byte(`]}`))
	if err != nil {
		return err
	}

	return nil
}

const discardBytesPreVector = 1 + 8 + 1 + 16 + 8 + 8

func extractPropsBytes(data []byte) ([]byte, error) {
	version := uint8(data[0])
	if version != 1 {
		return nil, errors.Errorf("unsupported binary marshaller version %d", version)
	}

	vecLen := binary.LittleEndian.Uint16(data[discardBytesPreVector : discardBytesPreVector+2])

	classNameStart := discardBytesPreVector + 2 + vecLen*4

	classNameLen := binary.LittleEndian.Uint16(data[classNameStart : classNameStart+2])

	propsLenStart := classNameStart + 2 + classNameLen
	propsLen := binary.LittleEndian.Uint32(data[propsLenStart : propsLenStart+4])

	start := int64(propsLenStart + 4)
	end := start + int64(propsLen)

	return data[start:end], nil
}
