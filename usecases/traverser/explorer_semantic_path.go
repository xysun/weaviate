package traverser

import (
	"context"
	"fmt"
)

const stepSize = 0.05

// IS ANSWER TEST
func (e *Explorer) semanticPath(source, target []float32) error {

	// Threshold
	threshold := float32(0.3)

	// Query and Target search for gulf of mexico
	// queryStringArr := []string{"Which river meets the Gulf of Mexico?"}
	// solutionSentenceArray := []string{
	// 	"Thousands of miles of levees, flood walls, and revetments have been erected to manage the Mississippi.", 
	// 	"Since the city wasn’t going to move to suit the river, the river would be forced to stay put.", 
	// 	"Southeast Louisiana, where the Mississippi River meets the Gulf of Mexico, as it is often depicted on maps.", 
	// 	"Directly next door to the Center for River Studies sits the headquarters of Louisiana’s Coastal Protection and Restoration Authority.", 
	// 	"The Corps plugged the Mississippi River Gulf Outlet with a nine-hundred-and-fifty-foot-wide rock dam and installed massive gates and pumps between the drainage canals and Lake Pontchartrain."}

	// Query which is more generic to show where it found value
	// queryStringArr := []string{"Is herbalife a pyramid scheme?"}
	// solutionSentenceArray := []string{
	// 	"Herbalife Nutrition is a global multi-level marketing corporation that develops and sells dietary supplements.",
	// 	"The company was founded by Mark Hughes in 1980, and it employs an estimated 8,900 people worldwide.",
	// 	"The business is incorporated in the Cayman Islands, with its corporate headquarters located in Los Angeles, California.",
	// 	"The company operates in 94 countries through a network of approximately 4.5 million independent distributors and members.",
	// 	"The company has been criticized by, among others, hedge fund manager Bill Ackman of Pershing Square Capital, who claimed that Herbalife operates a 'sophisticated pyramid scheme' after taking a $1 billion short position in Herbalife stock.",
	// 	"Herbalife agreed to 'fundamentally restructure' its business and pay a $200 million fine as part of a 2016 settlement with the U.S. Federal Trade Commission (FTC) following accusations of it being a pyramid scheme.",
	// 	"In November 2017, Ackman's hedge fund closed out its short position in Herbalife.",
	// 	"In February 1980, Mark Hughes began selling the original Herbalife weight management product from the trunk of his car.",
	// 	"Hughes often stated that the genesis of his product and program stemmed from the weight loss concerns of his mother Joanne, whose premature death he attributed to an eating disorder and an unhealthy approach to weight loss.",
	// 	"According to one Herbalife website, the company's goal was to change the nutritional habits of the world.",
	// 	"His first product was a protein shake designed to help people manage their weight.",
	// 	"He structured his company using a direct-selling, multi-level marketing model.",
	// 	"In 1982, Herbalife received complaints from the Food and Drug Administration for claims made about certain products and the inclusion of mandrake, poke root, and 'food grade' linseed oil in another.",
	// 	"As a result of the complaints, the company modified its product claims and reformulated the product."}

	// Wrong on purpose
	queryStringArr := []string{"Do birds fart?"}
	solutionSentenceArray := []string{
		"Thousands of miles of levees, flood walls, and revetments have been erected to manage the Mississippi.", 
		"Since the city wasn’t going to move to suit the river, the river would be forced to stay put.", 
		"Southeast Louisiana, where the Mississippi River meets the Gulf of Mexico, as it is often depicted on maps.", 
		"Directly next door to the Center for River Studies sits the headquarters of Louisiana’s Coastal Protection and Restoration Authority.", 
		"The Corps plugged the Mississippi River Gulf Outlet with a nine-hundred-and-fifty-foot-wide rock dam and installed massive gates and pumps between the drainage canals and Lake Pontchartrain."}

	// overwrite source and target
	queryVector, err := e.vectorizer.Corpi(context.TODO(), queryStringArr)
	if err != nil {
		return err
	}

	winner := "NO ANSWER FOUND... "
	winnerDist := float32(100.0)

	for _, singleSentence := range solutionSentenceArray {
		sentence := []string{singleSentence}
		vectorSentence, _ := e.vectorizer.Corpi(context.TODO(), sentence)
		distance, _ := e.distancer(queryVector, vectorSentence)
		if distance < winnerDist && distance < threshold {
			winner = singleSentence
			winnerDist = distance
		}
	}

	fmt.Println("============================================================")
	fmt.Printf("\033[1;36m%s\033[0m", "FULL CORPUS: ")
	fmt.Println(solutionSentenceArray)
	fmt.Printf("\033[1;36m%s\033[0m", "EXPLORE QUERY: ")
	fmt.Println(queryStringArr)
	fmt.Printf("\033[1;36m%s\033[0m", "WINNER SENTENCE: ")
	fmt.Println(winner, winnerDist)
	fmt.Println("============================================================")

	return nil
}

// IS SEMANTIC PATH TEST
// func (e *Explorer) semanticPath(source, target []float32) error {
// 	// Yoko Ono was the wife of Lennon
// 	sourceStringArr := []string{"beatles"}
// 	targetStringArr := []string{"yoko", "ono"}

// 	// Unknown relation
// 	// sourceStringArr := []string{"beatles"}
// 	// targetStringArr := []string{"iphone"}

// 	// // New York purchased the rights of NYC from Holland
// 	// sourceStringArr := []string{"new", "york", "city"}
// 	// targetStringArr := []string{"holland"}

// 	// // Fashion designer to fashion magazine
// 	// sourceStringArr := []string{"alexander", "mcqueen"}
// 	// targetStringArr := []string{"vogue"}

// 	// overwrite source and target
// 	source, err := e.vectorizer.Corpi(context.TODO(), sourceStringArr)
// 	if err != nil {
// 		return err
// 	}
// 	target, err = e.vectorizer.Corpi(context.TODO(), targetStringArr)
// 	if err != nil {
// 		return err
// 	}

// 	sourceWords, _, err := e.vectorizer.NearestWordsByVector(context.TODO(), source, 2500)
// 	if err != nil {
// 		return err
// 	}

// 	targetWords, _, err := e.vectorizer.NearestWordsByVector(context.TODO(), target, 2500)
// 	if err != nil {
// 		return err
// 	}

// 	// Moving away from the source as start point, towards the target
// 	lowestDistance := float32(9999.0)
// 	for i := range sourceWords {
// 		if string(sourceWords[i][0]) != "$" {
// 			sourceWordVector, err := e.vectorizer.Corpi(context.TODO(), []string{string(sourceWords[i])})
// 			if err != nil {
// 				return err
// 			}
// 			distToResult, _ := e.distancer(sourceWordVector, target)
// 			if distToResult < lowestDistance {
// 				fmt.Println("concept:", sourceWords[i], "distanceToQuery:", distToResult, "distanceToParent:", "UNSET", "distanceToNext:", "UNSET")
// 				lowestDistance = distToResult
// 			}
// 		}
// 	}

// 	// Moving towards the target, furthers away from the target
// 	for i := len(targetWords)-1; i >= 0; i-- {
// 		if string(targetWords[i][0]) != "$" {
// 			targetWordVector, err := e.vectorizer.Corpi(context.TODO(), []string{string(targetWords[i])})
// 			if err != nil {
// 				return err
// 			}
// 			distToResult, _ := e.distancer(targetWordVector, source)
// 			if distToResult < lowestDistance {
// 				fmt.Println("concept:", targetWords[i], "distanceToQuery", distToResult, "distanceToParent:", "UNSET", "distanceToNext:", "UNSET")
// 				lowestDistance = distToResult
// 			}
// 		}
// 	}

// 	return nil
// }

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
