package visited

import (
	"math/rand"
	"testing"
)

func BenchmarkInitialization(b *testing.B) {
	maxNodeID := int(10e6)
	entries := 100_000

	type bench struct {
		name     string
		initFunc func(maxNodeID, entries int)
	}

	benchs := []bench{
		{
			name: "init array based",
			initFunc: func(maxNodeID, entries int) {
				// The array-based implementation must be initialized with the maxNodeID
				// regardless of how many entries it will receive
				NewList(maxNodeID)
			},
		},
		{
			name: "init map based",
			initFunc: func(maxNodeID, entries int) {
				// The map-based implementation is agnostic of the maxID and can be
				// initialized with the expected number of entries
				NewMapBased(entries)
			},
		},
	}

	for _, bench := range benchs {
		b.Run(bench.name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				bench.initFunc(maxNodeID, entries)
			}
		})
	}
}

func BenchmarkVisit(b *testing.B) {
	maxNodeID := int(100e6)
	entries := int(100_000)

	type bench struct {
		name      string
		initFunc  func(maxNodeID, entries int) interface{}
		visitFunc func(b *testing.B, list interface{}, toVisit []uint64)
	}

	benchs := []bench{
		{
			name: "visit array based",
			initFunc: func(maxNodeID, entries int) interface{} {
				// The array-based implementation must be initialized with the maxNodeID
				// regardless of how many entries it will receive
				return NewList(maxNodeID)
			},
			visitFunc: func(b *testing.B, list interface{}, toVisit []uint64) {
				b.StopTimer()
				typed := list.(*List)
				b.StartTimer()

				for _, node := range toVisit {
					typed.Visit(node)
				}
			},
		},
		{
			name: "visit map based",
			initFunc: func(maxNodeID, entries int) interface{} {
				// The map-based implementation is agnostic of the maxID and can be
				// initialized with the expected number of entries
				return NewMapBased(entries)
			},
			visitFunc: func(b *testing.B, list interface{}, toVisit []uint64) {
				b.StopTimer()
				typed := list.(MapBasedVisitedList)
				b.StartTimer()

				for _, node := range toVisit {
					typed.Visit(node)
				}
			},
		},
	}

	for _, bench := range benchs {
		b.Run(bench.name, func(b *testing.B) {
			toVisit := make([]uint64, entries)
			for i := range toVisit {
				node := rand.Intn(maxNodeID)
				toVisit[i] = uint64(node)
			}
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				list := bench.initFunc(maxNodeID, entries)
				b.StartTimer()

				bench.visitFunc(b, list, toVisit)
			}
		})
	}
}

func BenchmarkVisited(b *testing.B) {
	maxNodeID := int(100e6)
	entries := int(100_000)

	type bench struct {
		name      string
		initFunc  func(maxNodeID, entries int) interface{}
		visitFunc func(b *testing.B, list interface{}, toVisit []uint64)
		checkFunc func(b *testing.B, list interface{}, toCheck []uint64)
	}

	benchs := []bench{
		{
			name: "visited array based",
			initFunc: func(maxNodeID, entries int) interface{} {
				// The array-based implementation must be initialized with the maxNodeID
				// regardless of how many entries it will receive
				return NewList(maxNodeID)
			},
			visitFunc: func(b *testing.B, list interface{}, toVisit []uint64) {
				typed := list.(*List)
				for _, node := range toVisit {
					typed.Visit(node)
				}
			},
			checkFunc: func(b *testing.B, list interface{}, toCheck []uint64) {
				b.StopTimer()
				typed := list.(*List)
				b.StartTimer()

				for _, node := range toCheck {
					typed.Visited(node)
				}
			},
		},
		{
			name: "visited map based",
			initFunc: func(maxNodeID, entries int) interface{} {
				// The map-based implementation is agnostic of the maxID and can be
				// initialized with the expected number of entries
				return NewMapBased(entries)
			},
			visitFunc: func(b *testing.B, list interface{}, toVisit []uint64) {
				typed := list.(MapBasedVisitedList)
				for _, node := range toVisit {
					typed.Visit(node)
				}
			},
			checkFunc: func(b *testing.B, list interface{}, toCheck []uint64) {
				b.StopTimer()
				typed := list.(MapBasedVisitedList)
				b.StartTimer()

				for _, node := range toCheck {
					typed.Visited(node)
				}
			},
		},
	}

	for _, bench := range benchs {
		b.Run(bench.name, func(b *testing.B) {
			toVisit := make([]uint64, entries)
			for i := range toVisit {
				node := rand.Intn(maxNodeID)
				toVisit[i] = uint64(node)
			}
			toCheck := make([]uint64, entries)
			for i := range toVisit {
				node := rand.Intn(maxNodeID)
				toVisit[i] = uint64(node)
			}

			// the idea is that this way we will visit about 50% known ids and about
			// 50% unknown ids, this should make this test a bit more balanced
			totalCheck := append(toVisit, toCheck...)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				list := bench.initFunc(maxNodeID, entries)
				bench.visitFunc(b, list, toVisit)
				b.StartTimer()

				bench.checkFunc(b, list, totalCheck)

			}
		})
	}
}
