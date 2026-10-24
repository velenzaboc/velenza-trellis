#!/usr/bin/env bash
# Run the integration-only source tier against one privately owned emulator.
set -euo pipefail

cd "$(dirname "$0")/.."
scratch_dir="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/test-integration.XXXXXX")"
cid_file="$scratch_dir/container.id"
helper_file="$scratch_dir/select-tests.go"
unit_files="$scratch_dir/unit-files"
tagged_files="$scratch_dir/tagged-files"

cleanup() {
  local result=$? cleanup_failed=0 container_id remaining
  trap - EXIT INT TERM
  set +e
  if [[ -s "$cid_file" ]]; then
    container_id="$(<"$cid_file")"
    if [[ "$container_id" =~ ^[0-9a-f]{64}$ ]]; then
      if ! timeout 15s docker container rm --force "$container_id"; then
        echo "Failed to remove owned emulator $container_id" >&2
        cleanup_failed=1
      fi
      if ! remaining="$(timeout 10s docker container ls --all --quiet --no-trunc --filter "id=$container_id")"; then
        echo "Could not verify owned emulator removal" >&2
        cleanup_failed=1
      elif [[ -n "$remaining" ]]; then
        echo "Owned emulator remains after cleanup: $remaining" >&2
        cleanup_failed=1
      fi
    else
      echo "Invalid owned container ID; refusing cleanup by name" >&2
      cleanup_failed=1
    fi
  fi
  if ! rm -f "$cid_file" "$helper_file" "$unit_files" "$tagged_files" || ! rmdir "$scratch_dir"; then
    echo "Could not remove owned integration scratch directory" >&2
    cleanup_failed=1
  fi
  if [[ "$result" == 0 && "$cleanup_failed" != 0 ]]; then result=1; fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for tool in go docker curl timeout bash sleep; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "Required integration tool is unavailable: $tool" >&2
    exit 1
  fi
done

# Build tags, rather than filename conventions or test-name prefixes, own tier membership.
go list -f '{{range .TestGoFiles}}{{println .}}{{end}}' . >"$unit_files"
go list -tags=integration -f '{{range .TestGoFiles}}{{println .}}{{end}}' . >"$tagged_files"
integration_files=()
while IFS= read -r source || [[ -n "$source" ]]; do
  [[ -n "$source" ]] || continue
  in_unit=false
  while IFS= read -r unit || [[ -n "$unit" ]]; do
    if [[ "$source" == "$unit" ]]; then in_unit=true; break; fi
  done <"$unit_files"
  if [[ "$in_unit" == false ]]; then integration_files+=("$source"); fi
done <"$tagged_files"
if [[ ${#integration_files[@]} == 0 ]]; then
  echo "No integration-only source files selected" >&2
  exit 1
fi

cat >"$helper_file" <<'GO'
package main

import (
  "fmt"
  "go/ast"
  "go/parser"
  "go/token"
  "os"
  "regexp"
  "sort"
  "strconv"
  "strings"
  "unicode"
  "unicode/utf8"
)

func reject() {
  fmt.Fprintln(os.Stderr, "invalid or ambiguous integration test inventory")
  os.Exit(1)
}

func main() {
  args := os.Args[1:]
  if len(args) < 2 || args[0] != "--" { reject() }
  names := make(map[string]bool)
  for _, path := range args[1:] {
    file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
    if err != nil { reject() }
    aliases := make(map[string]bool)
    for _, item := range file.Imports {
      imported, err := strconv.Unquote(item.Path.Value)
      if err != nil { reject() }
      if imported == "testing" {
        alias := "testing"
        if item.Name != nil { alias = item.Name.Name }
        aliases[alias] = true
      }
    }
    for _, declaration := range file.Decls {
      fn, ok := declaration.(*ast.FuncDecl)
      if !ok || fn.Recv != nil { continue }
      name := fn.Name.Name
      if !strings.HasPrefix(name, "Test") || name == "TestMain" { continue }
      if len(name) > len("Test") {
        next, _ := utf8.DecodeRuneInString(name[len("Test"):])
        if unicode.IsLower(next) { continue }
      }
      if fn.Type.TypeParams != nil || fn.Type.Params == nil || len(fn.Type.Params.List) != 1 ||
        (fn.Type.Results != nil && len(fn.Type.Results.List) != 0) { reject() }
      parameter := fn.Type.Params.List[0]
      if len(parameter.Names) > 1 { reject() }
      pointer, ok := parameter.Type.(*ast.StarExpr)
      if !ok { reject() }
      valid := false
      switch target := pointer.X.(type) {
      case *ast.SelectorExpr:
        pkg, ok := target.X.(*ast.Ident)
        valid = ok && pkg.Name != "_" && aliases[pkg.Name] && target.Sel.Name == "T"
      case *ast.Ident:
        valid = aliases["."] && target.Name == "T"
      }
      if !valid || names[name] { reject() }
      names[name] = true
    }
  }
  if len(names) == 0 { reject() }
  sorted := make([]string, 0, len(names))
  for name := range names { sorted = append(sorted, regexp.QuoteMeta(name)) }
  sort.Strings(sorted)
  fmt.Printf("^(%s)$\n", strings.Join(sorted, "|"))
}
GO

# The separator keeps selected .go paths out of go run's source-file arguments.
selection="$(go run "$helper_file" -- "${integration_files[@]}")"
if [[ -z "$selection" ]]; then
  echo "Empty integration test selection" >&2
  exit 1
fi

occupied=false
if curl --silent --show-error --max-time 2 --output /dev/null http://127.0.0.1:9020/; then occupied=true; fi
if timeout 2s bash -c 'exec 3<>/dev/tcp/127.0.0.1/9010'; then occupied=true; fi
if [[ "$occupied" == true ]]; then
  echo "Required emulator endpoint is already occupied; refusing to touch its owner" >&2
  exit 1
fi

if [[ -n "${GITHUB_RUN_ID:-}" && -n "${GITHUB_RUN_ATTEMPT:-}" ]]; then
  container_name="harness-review-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
else
  container_name="integration-${scratch_dir##*/}"
fi
docker container create --cidfile "$cid_file" --name "$container_name" \
  --publish 127.0.0.1:9010:9010 --publish 127.0.0.1:9020:9020 \
  gcr.io/cloud-spanner-emulator/emulator@sha256:c6f3402f2599684f295a0fdefb6fbbbfb18a0e43e309ff5456ccb452a4570a79
container_id="$(<"$cid_file")"
if [[ ! "$container_id" =~ ^[0-9a-f]{64}$ ]]; then
  echo "Invalid owned container ID; refusing emulator start" >&2
  exit 1
fi
present="$(timeout 10s docker container ls --all --quiet --no-trunc --filter "id=$container_id")"
if [[ "$present" != "$container_id" ]]; then
  echo "Could not verify the newly created emulator" >&2
  exit 1
fi
docker container start "$container_id"
ready=false
for attempt in {1..30}; do
  if curl --silent --show-error --max-time 2 --output /dev/null http://127.0.0.1:9020/ \
    && timeout 2s bash -c 'exec 3<>/dev/tcp/127.0.0.1/9010'; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  timeout 10s docker container logs "$container_id" || true
  echo "Spanner emulator readiness deadline exceeded" >&2
  exit 1
fi

SPANNER_EMULATOR_HOST=localhost:9010 go test -tags=integration . -run "$selection" -count=1
