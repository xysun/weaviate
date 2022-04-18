package storobj

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/additional"
)

func BenchmarkStorageObjectOld(b *testing.B) {
	_, asBinary := marshalExampleClassToBinary(b)

	b.ReportAllocs()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		FromBinaryOptional(asBinary, additional.Properties{})
	}
}

func BenchmarkStorageObjectLowAlloc(b *testing.B) {
	_, asBinary := marshalExampleClassToBinary(b)

	b.ReportAllocs()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		FromBinaryOptionalLowAlloc(asBinary, additional.Properties{})
	}
}
