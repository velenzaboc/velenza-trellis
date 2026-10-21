//go:build !windows

package gcpauth

import (
	"os"
	"testing"
)

const nonOwnerReadableCredentialError = "owner-only"

func configureCredentialFixtureAccess(_ *testing.T, _ string, _ os.FileMode) {}

func credentialSymlinkFixtureUnavailable(_ error) bool { return false }
