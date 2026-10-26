package inner

import "strings"

type MsgType int

const (
	MsgData MsgType = iota + 1
	MsgEOF
)

type QueryID int

const (
	Q1 QueryID = 1
	Q2 QueryID = 2
	Q3 QueryID = 3
	Q4 QueryID = 4
	Q5 QueryID = 5
)

func ExtractGatewayID(clientID string) string {
	idx := strings.LastIndex(clientID, "_")
	if idx > 0 {
		return clientID[:idx]
	}
	return clientID
}
