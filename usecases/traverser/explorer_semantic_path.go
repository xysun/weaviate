package traverser

import (
	"context"
	"fmt"
)

const stepSize = 0.05

func (e *Explorer) semanticPath(source, target []float32) error {
	// dist, err := e.distancer(source, target)
	// if err != nil {
	// 	return fmt.Errorf("distancer: calculate distance: %v", err)
	// }

	// directionalVec := substrVector(source, target)

	// overwrite source and target
	source, err := e.vectorizer.Corpi(context.TODO(), []string{"beatles"})
	if err != nil {
		return err
	}
	target, err = e.vectorizer.Corpi(context.TODO(), []string{"rolling", "stones"})
	if err != nil {
		return err
	}

	sourceWords, _, err := e.vectorizer.NearestWordsByVector(context.TODO(), source, 300)
	if err != nil {
		return err
	}

	targetWords, _, err := e.vectorizer.NearestWordsByVector(context.TODO(), target, 300)
	if err != nil {
		return err
	}

	overlapSource := -1
	overlapTarget := -1

	for i := range sourceWords {
		if string(sourceWords[i][0]) != "$" {
			if pos, ok := containedIn(targetWords, sourceWords[i]); ok {
				overlapSource = i
				overlapTarget = pos
				break
			}
		}

		if string(targetWords[i][0]) != "$" {
			if pos, ok := containedIn(sourceWords, targetWords[i]); ok {
				overlapSource = pos
				overlapTarget = i
				break
			}
		}
	}

	sourceVectors, sourceOccs, _ := e.vectorizer.MultiVectorForWord(context.TODO(), sourceWords)
	targetVectors, targetOccs, _ := e.vectorizer.MultiVectorForWord(context.TODO(), targetWords)

	count := 0
	if overlapSource != -1 {
		fmt.Printf("first overlap is %s (sourcePos: %d, targetPos: %d)\n", sourceWords[overlapSource], overlapSource, overlapTarget)
		for i := 0; i < overlapSource; i++ {
			if string(sourceWords[i][0]) == "$" {
				continue
			}

			dist, _ := e.distancer(sourceVectors[i], target)
			fmt.Printf("%d;%s;%f;%d\n", count, sourceWords[i], dist, sourceOccs[i])
			count++
		}
		dist, _ := e.distancer(sourceVectors[overlapSource], target)
		fmt.Printf("%d;%s;%f;%d\n", count, sourceWords[overlapSource], dist, sourceOccs[overlapSource])
		count++
		for i := overlapTarget - 1; i >= 0; i-- {
			if string(targetWords[i][0]) == "$" {
				continue
			}

			dist, _ := e.distancer(targetVectors[i], target)
			fmt.Printf("%d;%s;%f;%d\n", count, targetWords[overlapSource], dist, targetOccs[overlapSource])
			count++
		}
		fmt.Printf("\n\n")

	} else {
		fmt.Println("no overlap")
	}

	return nil
}

func containedIn(haystack []string, needle string) (int, bool) {
	for i, word := range haystack {
		if needle == word {
			return i, true
		}
	}

	return -1, false
}

func substrVector(target, source []float32) []float32 {
	out := make([]float32, len(target))
	for i := range out {
		out[i] = target[i] - source[i]
	}

	return out
}

func (e *Explorer) semanticPathKNN(source, target []float32) error {
	dist, err := e.distancer(source, target)
	if err != nil {
		return fmt.Errorf("distancer: calculate distance: %v", err)
	}

	// steps := int(math.Floor(float64(dist) / stepSize))
	// segments := make([][]float32, steps)

	fmt.Printf("\noverall distance: %f\n", dist)

	// for i := 0; i < steps; i++ {
	// 	segments[i] = e.steppedVector(source, target, i, steps)
	// 	word, _, err := e.vectorizer.NearestWordsByVector(context.TODO(), segments[i], 1)
	// 	if err != nil {
	// 		return fmt.Errorf("segment %d: %v", i, err)
	// 	}

	// 	fmt.Printf("segment %d: %s\n", i, word)
	// }

	ctx := context.TODO()

	segment := 1
	start := source
	for {
		words, _, err := e.vectorizer.NearestWordsByVector(ctx, start, 10)
		if err != nil {
			return err
		}

		// fmt.Printf("%v\n\n\n", words)

		vectors, _, err := e.vectorizer.MultiVectorForWord(ctx, words)
		if err != nil {
			return err
		}

		winner := -1
		winningDistance := float32(1000)

		for i, vec := range vectors {
			if string(words[i][0]) == "$" {
				continue
			}
			dist, _ := e.distancer(vec, target)
			if dist < winningDistance {
				winner = i
				winningDistance = dist
			}
		}

		if equalVectors(start, vectors[winner]) {
			// in other words the same word as our starting point
			break
		}
		fmt.Printf("segment %d: %s (%f)\n", segment, words[winner], winningDistance)

		start = vectors[winner]
		segment++
	}

	return nil

}

func (e *Explorer) steppedVector(source, target []float32, step, total int) []float32 {
	weightSource := float32(total-step) / float32(total)
	weightTarget := float32(step) / float32(total)

	out := make([]float32, len(source))
	for i := range target {
		out[i] = weightSource*source[i] + weightTarget*target[i]
	}

	return out
}

func equalVectors(a, b []float32) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
