package scaling

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/stretchr/testify/assert"
)

func TestScalerScale(t *testing.T) {
	ctx := context.Background()
	t.Run("NoShardingState", func(t *testing.T) {
		scaler := newFakeFactory(0, 1, 0).Scaler()
		old := sharding.Config{Replicas: 1}
		_, err := scaler.Scale(ctx, "C", old, old)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "no sharding state")
	})
	t.Run("SameReplicationFactor", func(t *testing.T) {
		scaler := newFakeFactory(1, 2, 2).Scaler()
		old := sharding.Config{Replicas: 2}
		_, err := scaler.Scale(ctx, "C", old, old)
		assert.Nil(t, err)
	})
	t.Run("ScaleInNotSupported", func(t *testing.T) {
		scaler := newFakeFactory(1, 2, 2).Scaler()
		old := sharding.Config{Replicas: 2}
		new := sharding.Config{Replicas: 1}
		_, err := scaler.Scale(ctx, "C", old, new)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "not supported")
	})
}
