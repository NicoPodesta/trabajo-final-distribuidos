package tokenizer

import (
	"hash/fnv"

	"trabajo-final-distribuidos/common/messageprotocol/inner"
	"trabajo-final-distribuidos/common/middleware"
)

func wordShard(word string, amount int) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(word))
	return int(hasher.Sum32() % uint32(amount))
}

func (tokenizer *Tokenizer) publish(clientID string, result batchResult) error {
	if len(result.vocabulary) > 0 {
		msg, err := inner.SerializeVocabulary(clientID, result.vocabulary)
		if err := send(tokenizer.vocabularyQueue, msg, err); err != nil {
			return err
		}
	}
	if err := tokenizer.publishWordCounts(clientID, result.wordCounts); err != nil {
		return err
	}
	if len(result.historicalCounts) > 0 {
		msg, err := inner.SerializeHistoricalCounts(clientID, result.historicalCounts)
		if err := send(tokenizer.historicalCountsQueue, msg, err); err != nil {
			return err
		}
	}
	if len(result.recentArticles) > 0 {
		msg, err := inner.SerializeRecentArticles(clientID, result.recentArticles)
		return send(tokenizer.recentArticlesQueue, msg, err)
	}
	return nil
}

func (tokenizer *Tokenizer) publishWordCounts(clientID string, wordCounts []inner.WordCountItem) error {
	byShard := make([][]inner.WordCountItem, len(tokenizer.wordCountsQueues))
	for _, item := range wordCounts {
		shard := wordShard(item.Word, len(byShard))
		byShard[shard] = append(byShard[shard], item)
	}

	for shard, items := range byShard {
		if len(items) == 0 {
			continue
		}
		msg, err := inner.SerializeWordCounts(clientID, items)
		if err := send(tokenizer.wordCountsQueues[shard], msg, err); err != nil {
			return err
		}
	}
	return nil
}

func (tokenizer *Tokenizer) publishEOF(clientID string) error {
	msg, err := inner.SerializeVocabularyEOF(clientID)
	if err := send(tokenizer.vocabularyQueue, msg, err); err != nil {
		return err
	}

	for _, queue := range tokenizer.wordCountsQueues {
		msg, err = inner.SerializeWordCountsEOF(clientID)
		if err := send(queue, msg, err); err != nil {
			return err
		}
	}

	msg, err = inner.SerializeHistoricalCountsEOF(clientID)
	if err := send(tokenizer.historicalCountsQueue, msg, err); err != nil {
		return err
	}

	msg, err = inner.SerializeRecentArticlesEOF(clientID)
	return send(tokenizer.recentArticlesQueue, msg, err)
}

func send(queue middleware.Middleware, msg *middleware.Message, serializeErr error) error {
	if serializeErr != nil {
		return serializeErr
	}
	return queue.Send(*msg)
}
