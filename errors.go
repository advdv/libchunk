package libchunk

import (
	"errors"
	"strings"
)

type errCollector struct {
	errors []error
	C      chan error
}

//ErrCollect allows for reaping errors concurrently
//and easily check or concat errors into a single error
func ErrCollect() *errCollector {
	return &errCollector{C: make(chan error)}
}

//Collect will start collecting errors send over
//the error channel
func (col *errCollector) Collect() {
	for err := range col.C {
		if err == nil {
			continue
		}

		col.errors = append(col.errors, err)
	}
}

//returns nil if no errors are colecd or else lists
//the errors in different lines
func (col *errCollector) ErrorOrNil() error {
	if len(col.errors) < 1 {
		return nil
	}

	var msgs []string
	for _, err := range col.errors {
		msgs = append(msgs, err.Error())
	}

	return errors.New(strings.Join(msgs, "\n"))
}
