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
	if err := tokenizer.publishVocabulary(clientID, result.vocabulary); err != nil {
		return err
	}
	if err := tokenizer.publishWordCounts(clientID, result.wordCounts); err != nil {
		return err
	}
	msg, err := inner.SerializeHistoricalCounts(clientID, result.historicalCounts)
	if err := send(tokenizer.historicalCountsQueue, msg, err); err != nil {
		return err
	}
	msg, err = inner.SerializeRecentArticles(clientID, result.recentArticles)
	return send(tokenizer.recentArticlesQueue, msg, err)
}

func (tokenizer *Tokenizer) publishVocabulary(clientID string, vocabulary []inner.VocabularyItem) error {
	byShard := make([][]inner.VocabularyItem, len(tokenizer.vocabularyQueues))
	for _, item := range vocabulary {
		shard := wordShard(item.Word, len(byShard))
		byShard[shard] = append(byShard[shard], item)
	}

	for shard, items := range byShard {
		if len(items) == 0 {
			continue
		}
		msg, err := inner.SerializeVocabulary(clientID, items)
		if err := send(tokenizer.vocabularyQueues[shard], msg, err); err != nil {
			return err
		}
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

func (tokenizer *Tokenizer) publishEOF(clientID string, totalBatches uint64, coordinated bool) error {
	for _, queue := range tokenizer.vocabularyQueues {
		msg, err := inner.SerializeVocabularyEOF(clientID)
		if err := send(queue, msg, err); err != nil {
			return err
		}
	}

	for _, queue := range tokenizer.wordCountsQueues {
		msg, err := inner.SerializeWordCountsEOF(clientID)
		if err := send(queue, msg, err); err != nil {
			return err
		}
	}

	if !coordinated {
		return nil
	}

	msg, err := inner.SerializeHistoricalCountsEOF(clientID, totalBatches)
	if err := send(tokenizer.historicalCountsQueue, msg, err); err != nil {
		return err
	}

	msg, err = inner.SerializeRecentArticlesEOF(clientID, totalBatches)
	return send(tokenizer.recentArticlesQueue, msg, err)
}

func send(queue middleware.Middleware, msg *middleware.Message, serializeErr error) error {
	if serializeErr != nil {
		return serializeErr
	}
	return queue.Send(*msg)
}
