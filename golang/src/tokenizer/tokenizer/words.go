package tokenizer

import (
	"regexp"
	"strings"
)

var wordRegex = regexp.MustCompile(`[\p{L}\p{N}_]+`)
var numberRegex = regexp.MustCompile(`^\p{Nd}+$`)

func extractWords(text string, language Language) []string {
	stopWords := stopWordsByLanguage[language]
	words := []string{}
	for _, word := range wordRegex.FindAllString(strings.ToLower(text), -1) {
		if _, ok := stopWords[word]; ok || numberRegex.MatchString(word) {
			continue
		}
		words = append(words, word)
	}
	return words
}

func distinctWords(words []string) []string {
	seen := make(map[string]struct{})
	distinct := []string{}
	for _, word := range words {
		if _, ok := seen[word]; ok {
			continue
		}
		seen[word] = struct{}{}
		distinct = append(distinct, word)
	}
	return distinct
}
