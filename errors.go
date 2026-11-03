package taskq

import "errors"

// permanentError marks a handler error as not worth retrying.
type permanentError struct{ err error }

func (p *permanentError) Error() string { return "permanent: " + p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Permanent wraps err so the worker moves the job straight to the dead-letter
// set instead of retrying it (e.g. a malformed payload that will never parse).
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err (or anything it wraps) was marked with
// Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
