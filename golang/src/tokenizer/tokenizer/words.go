package tokenizer

import (
	"regexp"
	"strings"
)

var wordRegex = regexp.MustCompile(`[\p{L}\p{N}_]+`)

func extractWords(text string) []string {
	seen := make(map[string]struct{})
	words := []string{}
	for _, word := range wordRegex.FindAllString(strings.ToLower(text), -1) {
		if _, ok := seen[word]; ok {
			continue
		}
		seen[word] = struct{}{}
		words = append(words, word)
	}
	return words
}
