package diskAnn

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	t.Run("Contains/Add", func(t *testing.T) {
		set := NewSet2()
		set.Add(4)
		set.Add(5)
		require.Equal(t, set.Contains(4), true)
		require.Equal(t, set.Contains(5), true)
		require.Equal(t, set.Contains(6), false)
		set.Remove(4)
		require.Equal(t, set.Contains(4), false)
		require.Equal(t, set.Contains(5), true)
		set.AddRange([]uint64{8, 9})
		require.Equal(t, set.Contains(5), true)
		require.Equal(t, set.Contains(8), true)
		require.Equal(t, set.Contains(9), true)
		set.RemoveRange([]uint64{5, 9})
		require.Equal(t, set.Contains(5), false)
		require.Equal(t, set.Contains(8), true)
		require.Equal(t, set.Contains(9), false)
		require.Equal(t, set.Size(), 1)
	})
}
