package taskq

import (
	"errors"
	"fmt"
	"testing"
)

func TestPermanent(t *testing.T) {
	base := errors.New("bad payload")
	p := Permanent(base)
	if !IsPermanent(p) || !errors.Is(p, base) {
		t.Fatal("Permanent must be detectable and unwrap to its cause")
	}
	if !IsPermanent(fmt.Errorf("handler: %w", p)) {
		t.Fatal("wrapping must preserve permanence")
	}
	if IsPermanent(base) || IsPermanent(nil) {
		t.Fatal("plain errors are retryable")
	}
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
}
