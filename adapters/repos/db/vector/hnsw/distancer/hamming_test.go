package distancer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHamming(t *testing.T) {
	t.Run("single byte - identical", func(t *testing.T) {
		a := []byte{byte(0b10101010)}
		b := []byte{byte(0b10101010)}

		dist, _, err := NewHammingProvider().SingleDist(a, b)
		require.Nil(t, err)
		assert.Equal(t, 0, dist)
	})

	t.Run("single byte - inverse", func(t *testing.T) {
		a := []byte{byte(0b10101010)}
		b := []byte{byte(0b01010101)}

		dist, _, err := NewHammingProvider().SingleDist(a, b)
		require.Nil(t, err)
		assert.Equal(t, 8, dist)
	})

	t.Run("single byte - mixed", func(t *testing.T) {
		a := []byte{byte(0b00111100)}
		b := []byte{byte(0b11110000)}

		dist, _, err := NewHammingProvider().SingleDist(a, b)
		require.Nil(t, err)
		assert.Equal(t, 4, dist)
	})

	t.Run("longer chain - identical", func(t *testing.T) {
		a := []byte{
			byte(0b10101010),
			byte(0b10101010),
			byte(0b10101010),
		}
		b := []byte{
			byte(0b10101010),
			byte(0b10101010),
			byte(0b10101010),
		}

		dist, _, err := NewHammingProvider().SingleDist(a, b)
		require.Nil(t, err)
		assert.Equal(t, 0, dist)
	})

	t.Run("longer chain - inverse", func(t *testing.T) {
		a := []byte{
			byte(0b10101010),
			byte(0b11111111),
			byte(0b00000000),
			byte(0b01010101),
		}
		b := []byte{
			byte(0b01010101),
			byte(0b00000000),
			byte(0b11111111),
			byte(0b10101010),
		}

		dist, _, err := NewHammingProvider().SingleDist(a, b)
		require.Nil(t, err)
		assert.Equal(t, 32, dist)
	})

	t.Run("long chain - mixed", func(t *testing.T) {
		a := []byte{
			byte(0b00111100),
			byte(0b11110000),
			byte(0b00111100),
			byte(0b11110000),
		}
		b := []byte{
			byte(0b11110000),
			byte(0b00111100),
			byte(0b11110000),
			byte(0b00111100),
		}

		dist, _, err := NewHammingProvider().SingleDist(a, b)
		require.Nil(t, err)
		assert.Equal(t, 16, dist)
	})
}
