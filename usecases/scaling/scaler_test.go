package scaling

import (
	"context"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/stretchr/testify/assert"
)

func TestScalerScale(t *testing.T) {
	ctx := context.Background()
	t.Run("NoShardingState", func(t *testing.T) {
		scaler := newFakeFactory(0, 1, 0).Scaler("")
		old := sharding.Config{}
		_, err := scaler.Scale(ctx, "C", old, 1, 2)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "no sharding state")
	})
	t.Run("SameReplicationFactor", func(t *testing.T) {
		scaler := newFakeFactory(1, 2, 2).Scaler("")
		old := sharding.Config{}
		_, err := scaler.Scale(ctx, "C", old, 2, 2)
		assert.Nil(t, err)
	})
	t.Run("ScaleInNotSupported", func(t *testing.T) {
		scaler := newFakeFactory(1, 2, 2).Scaler("")
		old := sharding.Config{}
		_, err := scaler.Scale(ctx, "C", old, 2, 1)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "not supported")
	})
}

func TestScalerScaleOut(t *testing.T) {
	var (
		dataDir = t.TempDir()
		ctx     = context.Background()
		cls     = "C"
		old     = sharding.Config{}
		bak     = backup.ClassDescriptor{
			Name: "C",
			Shards: []backup.ShardDescriptor{
				{
					Name: "S1", Files: []string{"f1"},
					PropLengthTrackerPath: "f4",
					ShardVersionPath:      "f4",
					DocIDCounterPath:      "f4",
				},
			},
		}
	)
	for i := 1; i < 5; i++ {
		file, err := os.Create(path.Join(dataDir, "f"+strconv.Itoa(i)))
		assert.Nil(t, err)
		file.Close()
	}
	f := newFakeFactory(1, 2, 1)
	f.Source.On("ShardsBackup", anyVal, anyVal, cls, []string{"S1"}).Return(bak, nil)
	f.Client.On("CreateShard", anyVal, "H2", cls, "S1").Return(nil)
	f.Client.On("PutFile", anyVal, "H2", cls, "S1", "f1", anyVal).Return(nil)
	f.Client.On("PutFile", anyVal, "H2", cls, "S1", "f4", anyVal).Return(nil)
	f.Client.On("ReInitShard", anyVal, "H2", cls, "S1").Return(nil)
	f.Source.On("ReleaseBackup", anyVal, anyVal, "C").Return(nil)
	scaler := f.Scaler(dataDir)
	_, err := scaler.Scale(ctx, "C", old, 1, 2)
	assert.Nil(t, err)
}

func TestDifference(t *testing.T) {
	tests := []struct {
		xs, ys, zs []string
	}{
		{
			zs: []string{},
		},
		{
			xs: []string{"1", "2"},
			ys: []string{},
			zs: []string{"1", "2"},
		},
		{
			xs: []string{"1", "2"},
			ys: []string{"1", "2"},
			zs: []string{},
		},
		{
			xs: []string{"1", "2", "3", "4"},
			ys: []string{"1", "3"},
			zs: []string{"2", "4"},
		},
		{
			xs: []string{"1", "2", "3", "4"},
			ys: []string{"2", "4"},
			zs: []string{"1", "3"},
		},
	}
	for _, c := range tests {
		assert.Equal(t, c.zs, difference(c.xs, c.ys))
	}
}
