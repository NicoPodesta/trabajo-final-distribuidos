package external

type MsgType uint32

const (
	ArticleRecord MsgType = iota + 1
	Ack
	EndOfRecords
	GatewayAssignment
	Q1Result
	Q2Result
	Q3Result
	Q4Result
	Q5Result
	Q2ImageChunk
)
