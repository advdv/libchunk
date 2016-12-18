package libchunk_test

import (
	"fmt"
	"testing"

	"github.com/advanderveer/libchunk"
)

func TestCollectOneError(t *testing.T) {
	col := libchunk.ErrCollect()
	go col.Collect()
	col.C <- fmt.Errorf("some error")

	err := col.ErrorOrNil()
	if err == nil {
		t.Error("should have resulted in an error")
	}
}

func TestCollectNoError(t *testing.T) {
	col := libchunk.ErrCollect()
	go col.Collect()

	err := col.ErrorOrNil()
	if err != nil {
		t.Error("should have resulted in no error")
	}

	col.C <- nil

	err = col.ErrorOrNil()
	if err != nil {
		t.Error("should have resulted in no error")
	}
}
