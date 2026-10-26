package tokenizer

import (
	"regexp"
	"strings"
)

var wordRegex = regexp.MustCompile(`[\p{L}\p{N}_]+`)
var numberRegex = regexp.MustCompile(`^\p{Nd}+$`)

func extractWords(text string, language Language) []string {
	stopWords := stopWordsByLanguage[language]
	seen := make(map[string]struct{})
	words := []string{}
	for _, word := range wordRegex.FindAllString(strings.ToLower(text), -1) {
		if _, ok := stopWords[word]; ok || numberRegex.MatchString(word) {
			continue
		}
		if _, ok := seen[word]; ok {
			continue
		}
		seen[word] = struct{}{}
		words = append(words, word)
	}
	return words
}
