package watermark

type Watermark struct {
	next  uint64
	ahead map[uint64]struct{}
}

func New() *Watermark {
	return &Watermark{ahead: make(map[uint64]struct{})}
}

func (watermark *Watermark) Seen(index uint64) bool {
	if index < watermark.next {
		return true
	}
	_, seen := watermark.ahead[index]
	return seen
}

func (watermark *Watermark) Add(index uint64) bool {
	if watermark.Seen(index) {
		return false
	}
	if index > watermark.next {
		watermark.ahead[index] = struct{}{}
		return true
	}

	watermark.next++
	for {
		if _, seen := watermark.ahead[watermark.next]; !seen {
			return true
		}
		delete(watermark.ahead, watermark.next)
		watermark.next++
	}
}

func (watermark *Watermark) Contiguous() uint64 {
	return watermark.next
}
