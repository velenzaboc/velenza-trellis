package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/velenzaboc/velenza-trellis/internal/gcpauth"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// adminClient wraps the Spanner Admin API for one substrate and uses the same
// closed credential selector as the data-plane client. No gcloud shell-out.
type adminClient struct {
	client *database.DatabaseAdminClient
}

func newAdminClient(ctx context.Context, credentials gcpauth.Selector, allowEmulator bool) (*adminClient, error) {
	var clientOptions []option.ClientOption
	useEmulator, err := gcpauth.ValidateSpannerEmulatorHost(os.Getenv("SPANNER_EMULATOR_HOST"), allowEmulator)
	if err != nil {
		return nil, fmt.Errorf("admin operations: %w", err)
	}
	if useEmulator {
		clientOptions = append(clientOptions, option.WithoutAuthentication())
	} else {
		tokenSource, err := credentials.TokenSource(ctx, gcpauth.CloudPlatformScope)
		if err != nil {
			return nil, fmt.Errorf("admin operations: google credentials: %w", err)
		}
		clientOptions = append(clientOptions, option.WithTokenSource(tokenSource))
	}
	c, err := database.NewDatabaseAdminClient(ctx, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("admin client: %w", err)
	}
	return &adminClient{client: c}, nil
}

func (a *adminClient) Close() error { return a.client.Close() }

// CreateDatabase issues CREATE DATABASE and waits for it to complete. The
// database name comes from the resource path; the rest of the path provides
// the parent instance.
func (a *adminClient) CreateDatabase(ctx context.Context, resourcePath string) error {
	_, _, database, err := parseSpannerResource(resourcePath)
	if err != nil {
		return err
	}
	parent := strings.TrimSuffix(resourcePath, "/databases/"+database)
	op, err := a.client.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          parent,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", database),
	})
	if err != nil {
		return fmt.Errorf("create database %q: %w", database, err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("create database %q wait: %w", database, err)
	}
	return nil
}

// DropDatabase deletes the database. Returns nil if it didn't exist.
func (a *adminClient) DropDatabase(ctx context.Context, resourcePath string) error {
	err := a.client.DropDatabase(ctx, &adminpb.DropDatabaseRequest{Database: resourcePath})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("drop database: %w", err)
	}
	return nil
}

// ApplyDDL issues UpdateDatabaseDdl with the given statements and waits.
// Statements are passed in the order received (DDL apply is order-sensitive).
func (a *adminClient) ApplyDDL(ctx context.Context, resourcePath string, statements []string) error {
	op, err := a.client.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   resourcePath,
		Statements: statements,
	})
	if err != nil {
		return fmt.Errorf("update ddl: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("update ddl wait: %w", err)
	}
	return nil
}

// splitDDL turns a DDL file's contents into a list of statements suitable for
// UpdateDatabaseDdl. Strips line comments (-- ...) and splits on top-level
// semicolons. Empty statements are dropped.
func splitDDL(src string) []string {
	// Strip line comments first.
	var lines []string
	for _, line := range strings.Split(src, "\n") {
		idx := strings.Index(line, "--")
		if idx >= 0 {
			line = line[:idx]
		}
		lines = append(lines, line)
	}
	cleaned := strings.Join(lines, "\n")
	// Split on ; — DDL has no embedded ; in string literals in our schema.
	parts := strings.Split(cleaned, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}
