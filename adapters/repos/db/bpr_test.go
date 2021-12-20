package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_VectorToBinary(t *testing.T) {
	t.Run("dim=8", func(t *testing.T) {
		in := []float32{
			0.3,
			0.1,
			-0.5,
			0.4,
			0.16,
			-0.8,
			0.16,
			-0.8,
		}

		expected := []byte{byte(0b11011010)}
		actual := vectorToBPRBinary(in)

		fmt.Printf("%b\n%b\n", expected, actual)

		assert.Equal(t, expected, actual)
	})
}
