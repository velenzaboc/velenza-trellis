//go:build windows

package gcpauth

import (
	"errors"
	"os"
	"os/exec"
	"testing"

	"golang.org/x/sys/windows"
)

const nonOwnerReadableCredentialError = "unapproved principal"

func configureCredentialFixtureAccess(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	configureCredentialACL(t, path)
	if mode.Perm()&0o077 == 0 {
		return
	}
	if output, err := exec.Command("icacls", path, "/grant", "Everyone:(R)").CombinedOutput(); err != nil {
		t.Fatalf("icacls non-owner-readable credential fixture: %v: %s", err, output)
	}
}

func credentialSymlinkFixtureUnavailable(err error) bool {
	return errors.Is(err, windows.ERROR_PRIVILEGE_NOT_HELD)
}
