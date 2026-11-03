//go:build integration

// Package integration runs taskq against a real Redis in a container.
package integration

import (
	"testing"

	"github.com/yousufmo27/taskq/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }
