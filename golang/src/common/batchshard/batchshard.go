package batchshard

func Of(batchID uint64, amount int) int {
	return int(batchID % uint64(amount))
}

type Sequence struct {
	first  uint64
	amount uint64
}

func NewSequence(shard, amount int) Sequence {
	first := uint64(shard)
	if first == 0 {
		first = uint64(amount)
	}
	return Sequence{first: first, amount: uint64(amount)}
}

func (sequence Sequence) Index(batchID uint64) (uint64, bool) {
	if batchID < sequence.first || (batchID-sequence.first)%sequence.amount != 0 {
		return 0, false
	}
	return (batchID - sequence.first) / sequence.amount, true
}

func (sequence Sequence) Count(total uint64) uint64 {
	if total < sequence.first {
		return 0
	}
	return (total-sequence.first)/sequence.amount + 1
}
