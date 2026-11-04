package completion

import (
	"errors"
	"fmt"
)

var (
	ErrZeroSequence     = errors.New("completion: sequences start at 1")
	ErrOutOfRange       = errors.New("completion: sequence exceeds final")
	ErrConflictingFinal = errors.New("completion: contradictory final")
	ErrNotObserved      = errors.New("completion: sequence has not been observed")
)

// El llamador publica el EOF y conserva el estado de cierre
type Tracker struct {
	observed    map[uint64]bool
	completed   uint64
	maxObserved uint64
	final       uint64
	hasFinal    bool
}

// Registra trabajo sin completarlo e ignora duplicados
func (t *Tracker) Observe(seq uint64) error {
	if err := t.validateSequence(seq); err != nil {
		return err
	}
	if t.observed == nil {
		t.observed = make(map[uint64]bool)
	}
	if _, exists := t.observed[seq]; !exists {
		t.observed[seq] = false
	}
	if seq > t.maxObserved {
		t.maxObserved = seq
	}
	return nil
}

// Marca trabajo observado como terminado, sin contar duplicados
func (t *Tracker) Complete(seq uint64) error {
	if err := t.validateSequence(seq); err != nil {
		return err
	}
	done, exists := t.observed[seq]
	if !exists {
		return fmt.Errorf("%w: %d", ErrNotObserved, seq)
	}
	if !done {
		t.observed[seq] = true
		t.completed++
	}
	return nil
}

// Fija el límite inclusivo, cero indica entrada vacía.
// Rechaza finales contradictorios y observaciones previas fuera de rango
func (t *Tracker) SetFinal(final uint64) error {
	if t.hasFinal && t.final != final {
		return fmt.Errorf("%w: have %d, received %d", ErrConflictingFinal, t.final, final)
	}
	if t.maxObserved > final {
		return fmt.Errorf("%w: observed %d, final %d", ErrOutOfRange, t.maxObserved, final)
	}
	t.final = final
	t.hasFinal = true
	return nil
}


func (t *Tracker) Final() (uint64, bool) {
	return t.final, t.hasFinal
}

func (t *Tracker) IsCompleted(seq uint64) bool {
	return t.observed[seq]
}

func (t *Tracker) IsComplete() bool {
	return t.hasFinal && t.completed == t.final
}

func (t *Tracker) validateSequence(seq uint64) error {
	if seq == 0 {
		return ErrZeroSequence
	}
	if t.hasFinal && seq > t.final {
		return fmt.Errorf("%w: sequence %d, final %d", ErrOutOfRange, seq, t.final)
	}
	return nil
}
