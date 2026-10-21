//go:build windows

package gcpauth

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

// configureCredentialACL constructs the exact minimal DACL the production
// check admits. icacls is Windows' native ACL tool and is present on the
// Windows runner that executes this build-constrained test.
func configureCredentialACL(t *testing.T, path string) {
	t.Helper()
	username := os.Getenv("USERNAME")
	if username == "" {
		t.Fatal("USERNAME is empty; cannot construct a user-owned credential ACL")
	}
	command := exec.Command("icacls", path,
		"/inheritance:r",
		"/grant:r",
		fmt.Sprintf("%s:(F)", username),
		"SYSTEM:(F)",
		"Administrators:(F)",
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("icacls minimal credential ACL: %v: %s", err, output)
	}
}

func openCredentialForAccessTest(t *testing.T) (*os.File, os.FileInfo) {
	t.Helper()
	path := t.TempDir() + "\\credential.json"
	if err := os.WriteFile(path, []byte(`{"type":"authorized_user"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	configureCredentialACL(t, path)
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return file, info
}

func TestValidateCredentialFileAccessWindowsAllowsOwnerSystemAndAdministrators(t *testing.T) {
	file, info := openCredentialForAccessTest(t)
	if err := validateCredentialFileAccess(file, info); err != nil {
		t.Fatalf("validateCredentialFileAccess(minimal DACL) = %v, want nil", err)
	}
}

func TestValidateCredentialFileAccessWindowsRejectsUnapprovedDACLPrincipal(t *testing.T) {
	file, info := openCredentialForAccessTest(t)
	if output, err := exec.Command("icacls", file.Name(), "/grant", "Everyone:(R)").CombinedOutput(); err != nil {
		t.Fatalf("icacls add unapproved principal: %v: %s", err, output)
	}
	err := validateCredentialFileAccess(file, info)
	if err == nil || !strings.Contains(err.Error(), "unapproved principal") {
		t.Fatalf("validateCredentialFileAccess(unapproved DACL) = %v, want unapproved-principal refusal", err)
	}
}

func TestValidateCredentialFileAccessWindowsRejectsNonUserOwner(t *testing.T) {
	currentUser, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatalf("resolve effective Windows user: %v", err)
	}
	administrators, err := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	if err != nil {
		t.Fatalf("resolve Administrators SID: %v", err)
	}
	err = validateCredentialFileOwner(administrators, currentUser.User.Sid)
	if err == nil || !strings.Contains(err.Error(), "owned by the effective user") {
		t.Fatalf("validateCredentialFileOwner(non-user owner) = %v, want owner refusal", err)
	}
}
