package traverser

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const stepSize = 0.05
const distanceThreshold = float32(0.3)

// IS ANSWER TEST
func (e *Explorer) semanticPath(source, target []float32) error {


	// Query and Target search for gulf of mexico
	queries := []string{
		"Which river meets the Gulf of Mexico?",
		"Is herbalife a pyramid scheme?",
		"Do birds fart?",
		"How much did the Manhattan Project cost?",
	}
	corpie := []string{`Thousands of miles of levees, flood walls, and revetments have been erected to manage the 
Mississippi. Since the city wasn’t going to move to suit the river, the river would be forced to stay put. Southeast 
Louisiana, where the Mississippi River meets the Gulf of Mexico, as it is often depicted on maps. Directly next door to 
the Center for River Studies sits the headquarters of Louisiana’s Coastal Protection and Restoration Authority. The 
Corps plugged the Mississippi River Gulf Outlet with a nine-hundred-and-fifty-foot-wide rock dam and installed massive 
gates and pumps between the drainage canals and Lake Pontchartrain.`,
		`Herbalife Nutrition is a global multi-level marketing corporation that develops and sells
dietary supplements. The company was founded by Mark Hughes in 1980, and it employs an estimated 8,900 people worldwide.
The business is incorporated in the Cayman Islands, with its corporate headquarters located in Los Angeles, California.
The company operates in 94 countries through a network of approximately 4.5 million independent distributors and
members. The company has been criticized by, among others, hedge fund manager Bill Ackman of Pershing Square Capital,
who claimed that Herbalife operates a 'sophisticated pyramid scheme' after taking a $1 billion short position in
Herbalife stock. Herbalife agreed to 'fundamentally restructure' its business and pay a $200 million fine as part of a
2016 settlement with the U.S. Federal Trade Commission (FTC) following accusations of it being a pyramid scheme. In
November 2017, Ackman's hedge fund closed out its short position in Herbalife. In February 1980, Mark Hughes began
selling the original Herbalife weight management product from the trunk of his car. Hughes often stated that the genesis
of his product and program stemmed from the weight loss concerns of his mother Joanne, whose premature death he
attributed to an eating disorder and an unhealthy approach to weight loss. According to one Herbalife website, the
company's goal was to change the nutritional habits of the world. His first product was a protein shake designed to help
people manage their weight. He structured his company using a direct-selling, multi-level marketing model. In 1982,
Herbalife received complaints from the Food and Drug Administration for claims made about certain products and the
inclusion of mandrake, poke root, and 'food grade' linseed oil in another. As a result of the complaints, the company
modified its product claims and reformulated the product.`,
		`Thousands of miles of levees, flood walls, and revetments have been erected to manage the
Mississippi. Since the city wasn’t going to move to suit the river, the river would be forced to stay put. Southeast
Louisiana, where the Mississippi River meets the Gulf of Mexico, as it is often depicted on maps. Directly next door to
the Center for River Studies sits the headquarters of Louisiana’s Coastal Protection and Restoration Authority. The
Corps plugged the Mississippi River Gulf Outlet with a nine-hundred-and-fifty-foot-wide rock dam and installed massive
gates and pumps between the drainage canals and Lake Pontchartrain.`,
`The Manhattan Project was a research and development undertaking during World War II that produced the first nuclear weapons. It was led by the United States with the support of the United Kingdom and Canada. From 1942 to 1946, the project was under the direction of Major General Leslie Groves of the U.S. Army Corps of Engineers. Nuclear physicist Robert Oppenheimer was the director of the Los Alamos Laboratory that designed the actual bombs. The Army component of the project was designated the Manhattan District; Manhattan gradually superseded the official codename, Development of Substitute Materials, for the entire project. Along the way, the project absorbed its earlier British counterpart, Tube Alloys. The Manhattan Project began modestly in 1939, but grew to employ more than 130,000 people and cost nearly US$2 billion (about $28 billion today[1]). Over 90% of the cost was for building factories and to produce fissile material, with less than 10% for development and production of the weapons. Research and production took place at more than 30 sites across the United States, the United Kingdom, and Canada. Two types of atomic bombs were developed concurrently during the war: a relatively simple gun-type fission weapon and a more complex implosion-type nuclear weapon. The Thin Man gun-type design proved impractical to use with plutonium, and therefore a simpler gun-type called Little Boy was developed that used uranium-235, an isotope that makes up only 0.7 percent of natural uranium. Chemically identical to the most common isotope, uranium-238, and with almost the same mass, it proved difficult to separate the two. Three methods were employed for uranium enrichment: electromagnetic, gaseous and thermal. Most of this work was performed at the Clinton Engineer Works at Oak Ridge, Tennessee. In parallel with the work on uranium was an effort to produce plutonium, which was discovered at the University of California in 1940.[2] After the feasibility of the world's first artificial nuclear reactor, the Chicago Pile-1, was demonstrated in 1942 at the Metallurgical Laboratory in the University of Chicago, the Project designed the X-10 Graphite Reactor at Oak Ridge and the production reactors at the Hanford Site in Washington state, in which uranium was irradiated and transmuted into plutonium. The plutonium was then chemically separated from the uranium, using the bismuth phosphate process. The Fat Man plutonium implosion-type weapon was developed in a concerted design and development effort by the Los Alamos Laboratory. The project was also charged with gathering intelligence on the German nuclear weapon project. Through Operation Alsos, Manhattan Project personnel served in Europe, sometimes behind enemy lines, where they gathered nuclear materials and documents, and rounded up German scientists. Despite the Manhattan Project's tight security, Soviet atomic spies successfully penetrated the program. The first nuclear device ever detonated was an implosion-type bomb at the Trinity test, conducted at New Mexico's Alamogordo Bombing and Gunnery Range on 16 July 1945. Little Boy and Fat Man bombs were used a month later in the atomic bombings of Hiroshima and Nagasaki, respectively. In the immediate postwar years, the Manhattan Project conducted weapons testing at Bikini Atoll as part of Operation Crossroads, developed new weapons, promoted the development of the network of national laboratories, supported medical research into radiology and laid the foundations for the nuclear navy. It maintained control over American atomic weapons research and production until the formation of the United States Atomic Energy Commission in January 1947. The discovery of nuclear fission by German chemists Otto Hahn and Fritz Strassmann in 1938, and its theoretical explanation by Lise Meitner and Otto Frisch, made the development of an atomic bomb a theoretical possibility. There were fears that a German atomic bomb project would develop one first, especially among scientists who were refugees from Nazi Germany and other fascist countries.[3] In August 1939, Hungarian-born physicists Leo Szilard and Eugene Wigner drafted the Einstein–Szilard letter, which warned of the potential development of "extremely powerful bombs of a new type". It urged the United States to take steps to acquire stockpiles of uranium ore and accelerate the research of Enrico Fermi and others into nuclear chain reactions. They had it signed by Albert Einstein and delivered to President Franklin D. Roosevelt. Roosevelt called on Lyman Briggs of the National Bureau of Standards to head the Advisory Committee on Uranium to investigate the issues raised by the letter. Briggs held a meeting on 21 October 1939, which was attended by Szilárd, Wigner and Edward Teller. The committee reported back to Roosevelt in November that uranium "would provide a possible source of bombs with a destructiveness vastly greater than anything now known."[4] The U.S. Navy awarded Columbia University $6,000 in funding, most of which Enrico Fermi and Szilard spent on purchasing graphite. A team of Columbia professors including Fermi, Szilard, Eugene T. Booth and John Dunning created the first nuclear fission reaction in the Americas, verifying the work of Hahn and Strassmann. The same team subsequently built a series of prototype nuclear reactors (or "piles" as Fermi called them) in Pupin Hall at Columbia, but were not yet able to achieve a chain reaction.[5] The Advisory Committee on Uranium became the National Defense Research Committee (NDRC) on Uranium when that organization was formed on 27 June 1940.[6] Briggs proposed spending $167,000 on research into uranium, particularly the uranium-235 isotope, and plutonium, which was discovered in 1940 at the University of California[2][7] On 28 June 1941, Roosevelt signed Executive Order 8807, which created the Office of Scientific Research and Development (OSRD),[8] with Vannevar Bush as its director. The office was empowered to engage in large engineering projects in addition to research.[7] The NDRC Committee on Uranium became the S-1 Section of the OSRD; the word "uranium" was dropped for security reasons.[9] In Britain, Frisch and Rudolf Peierls at the University of Birmingham had made a breakthrough investigating the critical mass of uranium-235 in June 1939.[10] Their calculations indicated that it was within an order of magnitude of 10 kilograms (22 lb), which was small enough to be carried by a bomber of the day.[11] Their March 1940 Frisch–Peierls memorandum initiated the British atomic bomb project and its MAUD Committee,[12] which unanimously recommended pursuing the development of an atomic bomb.[11] In July 1940, Britain had offered to give the United States access to its scientific research,[13] and the Tizard Mission's John Cockcroft briefed American scientists on British developments. He discovered that the American project was smaller than the British, and not as far advanced.[14] As part of the scientific exchange, the MAUD Committee's findings were conveyed to the United States. One of its members, the Australian physicist Mark Oliphant, flew to the United States in late August 1941 and discovered that data provided by the MAUD Committee had not reached key American physicists. Oliphant then set out to find out why the committee's findings were apparently being ignored. He met with the Uranium Committee and visited Berkeley, California, where he spoke persuasively to Ernest O. Lawrence. Lawrence was sufficiently impressed to commence his own research into uranium. He in turn spoke to James B. Conant, Arthur H. Compton and George B. Pegram. Oliphant's mission was therefore a success; key American physicists were now aware of the potential power of an atomic bomb.[15][16] On 9 October 1941, President Roosevelt approved the atomic program after he convened a meeting with Vannevar Bush and Vice President Henry A. Wallace. To control the program, he created a Top Policy Group consisting of himself—although he never attended a meeting—Wallace, Bush, Conant, Secretary of War Henry L. Stimson, and the Chief of Staff of the Army, General George C. Marshall. Roosevelt chose the Army to run the project rather than the Navy, because the Army had more experience with management of large-scale construction projects. He also agreed to coordinate the effort with that of the British, and on 11 October he sent a message to Prime Minister Winston Churchill, suggesting that they correspond on atomic matters. The S-1 Committee held its meeting on 18 December 1941 "pervaded by an atmosphere of enthusiasm and urgency"[18] in the wake of the attack on Pearl Harbor and the subsequent United States declaration of war upon Japan and then on Germany.[19] Work was proceeding on three different techniques for isotope separation to separate uranium-235 from the more abundant uranium-238. Lawrence and his team at the University of California,[2] investigated electromagnetic separation, while Eger Murphree and Jesse Wakefield Beams's team looked into gaseous diffusion at Columbia University, and Philip Abelson directed research into thermal diffusion at the Carnegie Institution of Washington and later the Naval Research Laboratory.[20] Murphree was also the head of an unsuccessful separation project using gas centrifuges.[21] Meanwhile, there were two lines of research into nuclear reactor technology, with Harold Urey continuing research into heavy water at Columbia, while Arthur Compton brought the scientists working under his supervision from Columbia, California and Princeton University to join his team at the University of Chicago, where he organized the Metallurgical Laboratory in early 1942 to study plutonium and reactors using graphite as a neutron moderator.[22] Briggs, Compton, Lawrence, Murphree, and Urey met on 23 May 1942 to finalize the S-1 Committee recommendations, which called for all five technologies to be pursued. This was approved by Bush, Conant, and Brigadier General Wilhelm D. Styer, the chief of staff of Major General Brehon B. Somervell's Services of Supply, who had been designated the Army's representative on nuclear matters.[20] Bush and Conant then took the recommendation to the Top Policy Group with a budget proposal for $54 million for construction by the United States Army Corps of Engineers, $31 million for research and development by OSRD and $5 million for contingencies in fiscal year 1943. The Top Policy Group in turn sent it on 17 June 1942 to the President, who approved it by writing "OK FDR" on the document.[20] Compton asked theoretical physicist J. Robert Oppenheimer of the University of California[2] to take over research into fast neutron calculations—the key to calculations of critical mass and weapon detonation—from Gregory Breit, who had quit on 18 May 1942 because of concerns over lax operational security.[23] John H. Manley, a physicist at the Metallurgical Laboratory, was assigned to assist Oppenheimer by contacting and coordinating experimental physics groups scattered across the country.[24] Oppenheimer and Robert Serber of the University of Illinois examined the problems of neutron diffusion—how neutrons moved in a nuclear chain reaction—and hydrodynamics—how the explosion produced by a chain reaction might behave. To review this work and the general theory of fission reactions, Oppenheimer and Fermi convened meetings at the University of Chicago in June and at the University of California in July 1942 with theoretical physicists Hans Bethe, John Van Vleck, Edward Teller, Emil Konopinski, Robert Serber, Stan Frankel, and Eldred C. Nelson, the latter three former students of Oppenheimer, and experimental physicists Emilio Segrè, Felix Bloch, Franco Rasetti, John Henry Manley, and Edwin McMillan. They tentatively confirmed that a fission bomb was theoretically possible.[25] There were still many unknown factors. The properties of pure uranium-235 were relatively unknown, as were those of plutonium, an element that had only been discovered in February 1941 by Glenn Seaborg and his team. The scientists at the (July 1942) Berkeley conference envisioned creating plutonium in nuclear reactors where uranium-238 atoms absorbed neutrons that had been emitted from fissioning uranium-235 atoms. At this point no reactor had been built, and only tiny quantities of plutonium were available from cyclotrons at institutions such as Washington University in St. Louis.[26] Even by December 1943, only two milligrams had been produced.[27] There were many ways of arranging the fissile material into a critical mass. The simplest was shooting a "cylindrical plug" into a sphere of "active material" with a "tamper"—dense material that would focus neutrons inward and keep the reacting mass together to increase its efficiency.[28] They also explored designs involving spheroids, a primitive form of "implosion" suggested by Richard C. Tolman, and the possibility of autocatalytic methods, which would increase the efficiency of the bomb as it exploded.[29] Considering the idea of the fission bomb theoretically settled—at least until more experimental data was available—the 1942 Berkeley conference then turned in a different direction. Edward Teller pushed for discussion of a more powerful bomb: the "super", now usually referred to as a "hydrogen bomb", which would use the explosive force of a detonating fission bomb to ignite a nuclear fusion reaction in deuterium and tritium.[30] Teller proposed scheme after scheme, but Bethe refused each one. The fusion idea was put aside to concentrate on producing fission bombs.[31] Teller also raised the speculative possibility that an atomic bomb might "ignite" the atmosphere because of a hypothetical fusion reaction of nitrogen nuclei.[note 1] Bethe calculated that it could not happen,[33] and a report co-authored by Teller showed that "no self-propagating chain of nuclear reactions is likely to be started."[34] In Serber's account, Oppenheimer mentioned the possibility of this scenario to Arthur Compton, who "didn't have enough sense to shut up about it. It somehow got into a document that went to Washington" and was "never laid to rest". Vannevar Bush became dissatisfied with Colonel Marshall's failure to get the project moving forward expeditiously, specifically the failure to acquire the Tennessee site, the low priority allocated to the project by the Army and the location of his headquarters in New York City.[46] Bush felt that more aggressive leadership was required, and spoke to Harvey Bundy and Generals Marshall, Somervell, and Styer about his concerns. He wanted the project placed under a senior policy committee, with a prestigious officer, preferably Styer, as overall director.[44] Somervell and Styer selected Groves for the post, informing him on 17 September of this decision, and that General Marshall ordered that he be promoted to brigadier general,[47] as it was felt that the title "general" would hold more sway with the academic scientists working on the Manhattan Project.[48] Groves' orders placed him directly under Somervell rather than Reybold, with Colonel Marshall now answerable to Groves.[49] Groves established his headquarters in Washington, D.C., on the fifth floor of the New War Department Building, where Colonel Marshall had his liaison office.[50] He assumed command of the Manhattan Project on 23 September 1942. Later that day, he attended a meeting called by Stimson, which established a Military Policy Committee, responsible to the Top Policy Group, consisting of Bush (with Conant as an alternate), Styer and Rear Admiral William R. Purnell.[47] Tolman and Conant were later appointed as Groves' scientific advisers.[51] On 19 September, Groves went to Donald Nelson, the chairman of the War Production Board, and asked for broad authority to issue a AAA rating whenever it was required. Nelson initially balked but quickly caved in when Groves threatened to go to the President.[52] Groves promised not to use the AAA rating unless it was necessary. It soon transpired that for the routine requirements of the project the AAA rating was too high but the AA-3 rating was too low. After a long campaign, Groves finally received AA-1 authority on 1 July 1944.[53] According to Groves, "In Washington you became aware of the importance of top priority. Most everything proposed in the Roosevelt administration would have top priority. That would last for about a week or two and then something else would get top priority".[54] One of Groves' early problems was to find a director for Project Y, the group that would design and build the bomb. The obvious choice was one of the three laboratory heads, Urey, Lawrence, or Compton, but they could not be spared. Compton recommended Oppenheimer, who was already intimately familiar with the bomb design concepts. However, Oppenheimer had little administrative experience, and, unlike Urey, Lawrence, and Compton, had not won a Nobel Prize, which many scientists felt that the head of such an important laboratory should have. There were also concerns about Oppenheimer's security status, as many of his associates were Communists, including his brother, Frank Oppenheimer; his wife, Kitty; and his girlfriend, Jean Tatlock. A long conversation on a train in October 1942 convinced Groves and Nichols that Oppenheimer thoroughly understood the issues involved in setting up a laboratory in a remote area and should be appointed as its director. Groves personally waived the security requirements and issued Oppenheimer a clearance on 20 July 1943.`,
	}


	speedTest := []time.Duration{}

	for i := 0; i < len(queries); i++ {
		corpus := corpie[i]

		// for predifined snippets
		//corpus := strings.Split(corpie[i], ".")

		query := queries[i]

		startTime := time.Now()

		// overwrite source and target
		queryVector, err := e.vectorizer.Corpi(context.TODO(), []string{query})
		if err != nil {
			return err
		}

		winner, winnerDist := e.searchByDevide(corpus, queryVector)
		//winner, winnerDist := e.searchByWord(corpus, queryVector)
		//winner, winnerDist := e.searchByPredefinedSnippets(corpus, queryVector)

		endTime := time.Now()
		elapsed := endTime.Sub(startTime)

		speedTest = append(speedTest, elapsed)

		printInfo(corpus, query, winner, winnerDist, elapsed)

		// for predifined snippets
		//printInfo(corpie[i], query, winner, winnerDist, elapsed)
	}

	for _, dur := range(speedTest) {
		fmt.Printf("Speeds:\n")
		fmt.Printf("%v\n", dur)
	}


	return nil
}

func printInfo(corpus string, query string, winner string, winnerDist float32, elapsed time.Duration) {
	fmt.Println("============================================================")
	fmt.Printf("\033[1;36m%s\033[0m", "FULL CORPUS: ")
	fmt.Println(corpus)
	fmt.Printf("\033[1;36m%s\033[0m", "EXPLORE QUERY: ")
	fmt.Println(query)
	fmt.Printf("\033[1;36m%s\033[0m", "WINNER SENTENCE: ")
	fmt.Println(winner, winnerDist)
	fmt.Printf("\033[1;36m%s\033[0m", "Time elapsed: ")
	fmt.Println(elapsed)
	fmt.Println("============================================================")
}

func (e *Explorer) searchByPredefinedSnippets(corpus []string, queryVector []float32) (string, float32){
	winnerDist := float32(100.0)
	winnerSnippet := "NO ANSWER FOUND... "

	for _, snippet := range(corpus) {
		vecSnippet, _ := e.vectorizer.Corpi(context.TODO(), []string{snippet})
		distance, _ := e.distancer(queryVector, vecSnippet)
		if distance < winnerDist && distance < distanceThreshold {
			winnerSnippet = snippet
			winnerDist = distance
		}
	}

	return winnerSnippet, winnerDist
}

func (e *Explorer) searchByWord(corpus string, queryVector []float32) (string, float32){
	winnerDist := float32(100.0)
	winnerWord := "NO ANSWER FOUND... "

	words := strings.Fields(corpus)
	for _, word := range words {
		vecWord, _ := e.vectorizer.Corpi(context.TODO(), []string{word})
		distance, _ := e.distancer(queryVector, vecWord)
		if distance < winnerDist && distance < distanceThreshold {
			winnerWord = word
			winnerDist = distance
		}
	}

	return winnerWord, winnerDist
}

func (e *Explorer) searchByDevide(corpus string, queryVector []float32) (string, float32){
	splitPosition := int(len(corpus) / 2)
	distance := float32(100.0)

	for len(corpus) > 50 {
		corpus, distance = e.getSmallerCorpus(corpus, queryVector, splitPosition)
		splitPosition = int(len(corpus) / 2)
	}

	if distance > distanceThreshold {
		return "NO ANSWER FOUND... ", float32(100.0)
	}

	return corpus, distance

}

func (e *Explorer) getSmallerCorpus(corpus string, queryVector []float32, splitPosition int) (string, float32) {
	left := corpus[:splitPosition]
	right := corpus[splitPosition:]
	vecLeft, _ := e.vectorizer.Corpi(context.TODO(), []string{left})
	vecRight, _ := e.vectorizer.Corpi(context.TODO(), []string{right})
	distanceLeft, _ := e.distancer(queryVector, vecLeft)
	distanceRight, _ := e.distancer(queryVector, vecRight)
	if distanceLeft > distanceRight {
		return right, distanceRight
	}
	return left, distanceLeft
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
