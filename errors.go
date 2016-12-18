package libchunk

import (
	"errors"
	"strings"
)

//ErrCollector allows for reaping errors concurrently
type ErrCollector []error

func (errs ErrCollector) Collect(errCh <-chan error) {
	for err := range errCh {
		errs = append(errs, err)
	}
}

//returns nil if no errors are collecd or else lists
//the errors with whitespace
func (errs ErrCollector) ErrorOrNil() error {
	if len(errs) < 1 {
		return nil
	}
	var msgs []string
	for _, err := range errs {
		msgs = append(msgs, err.Error())
	}

	return errors.New(strings.Join(msgs, "\n"))
}
