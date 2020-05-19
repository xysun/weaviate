package traverser

import (
	"context"
	"fmt"
	"time"
)

const stepSize = 0.05

// this is the latest approach, i.e. the "overlapping algo"
func (e *Explorer) semanticPath(source, target []float32) error {
	// // Yoko Ono was the wife of Lennon
	// sourceStringArr := []string{"beatles"}
	// targetStringArr := []string{"yoko", "ono"}

	// //// New York purchased the rights of NYC from Holland
	// // sourceStringArr := []string{"new", "york", "city"}
	// // targetStringArr := []string{"holland"}

	// //// Fashion designer to fashion magazine
	// // sourceStringArr := []string{"alexander", "mcqueen"}
	// // targetStringArr := []string{"vogue"}

	// // overwrite source and target
	// source, err := e.vectorizer.Corpi(context.TODO(), sourceStringArr)
	// if err != nil {
	// 	return err
	// }
	// target, err = e.vectorizer.Corpi(context.TODO(), targetStringArr)
	// if err != nil {
	// 	return err
	// }

	before := time.Now()
	sourceWords, _, err := e.vectorizer.NearestWordsByVector(context.TODO(), source, 2500)
	if err != nil {
		return err
	}

	targetWords, _, err := e.vectorizer.NearestWordsByVector(context.TODO(), target, 2500)
	if err != nil {
		return err
	}
	fmt.Printf("getting NNs took %s\n", time.Since(before))

	fmt.Printf("out of interest, lengths are %d and %d\n", len(sourceWords), len(targetWords))

	// Moving away from the source as start point, towards the target
	lowestDistance := float32(9999.0)
	for i := range sourceWords {
		if string(sourceWords[i][0]) != "$" {
			sourceWordVector, err := e.vectorizer.Corpi(context.TODO(), []string{string(sourceWords[i])})
			if err != nil {
				return err
			}
			distToResult, _ := e.distancer(sourceWordVector, target)
			if distToResult < lowestDistance {
				fmt.Println("concept:", sourceWords[i], "distanceToQuery:", distToResult, "distanceToParent:", "UNSET", "distanceToNext:", "UNSET")
				lowestDistance = distToResult
			}
		}
	}

	// Moving towards the target, furthers away from the target
	for i := len(targetWords) - 1; i >= 0; i-- {
		if string(targetWords[i][0]) != "$" {
			targetWordVector, err := e.vectorizer.Corpi(context.TODO(), []string{string(targetWords[i])})
			if err != nil {
				return err
			}
			distToResult, _ := e.distancer(targetWordVector, source)
			if distToResult < lowestDistance {
				fmt.Println("concept:", targetWords[i], "distanceToQuery", distToResult, "distanceToParent:", "UNSET", "distanceToNext:", "UNSET")
				lowestDistance = distToResult
			}
		}
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

// this is the alternative approach, i.e. doing a KNN search for each point
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
