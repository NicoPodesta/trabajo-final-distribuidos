package q1filter

import "trabajo-final-distribuidos/q1filter/protocol"

func (filter *Filter) publishOutput(output protocol.Output) error {
	msg, err := protocol.EncodeOutput(output)
	if err != nil {
		return err
	}
	return filter.outputQueue.Send(msg)
}
