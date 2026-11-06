#!/usr/bin/env bash
# Manual verification entrypoint, invoked only under the designated native resource guard.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -z "${VELENZA_VERIFY_HOST:-}" || "$(hostname -s)" != "$VELENZA_VERIFY_HOST" ]]; then echo 'Verification requires its designated guarded host' >&2; exit 75; fi
: "${VELENZA_VERIFY_DOCKER_CONTEXT:?Select the registered rootless build context}"
scope="$(python3 - <<'PY'
import pathlib, re
line=next(x[3:] for x in pathlib.Path('/proc/self/cgroup').read_text().splitlines() if x.startswith('0::'))
name=pathlib.PurePosixPath(line).name
if not re.fullmatch(r'velenza-preci-[0-9]+-[a-f0-9]+\.scope',name): raise SystemExit('Native resource-guard scope is required')
base=pathlib.Path('/sys/fs/cgroup')/line.lstrip('/')
quota,period=(base/'cpu.max').read_text().split()
if quota=='max' or not 0<int(quota)/int(period)<=4:raise SystemExit('CPU cap is required')
if not 0<int((base/'memory.max').read_text())<=8*1024**3:raise SystemExit('RAM cap is required')
if int((base/'memory.swap.max').read_text())!=0:raise SystemExit('No-swap cap is required')
print(name)
PY
)"
for variable in GOCACHE GOMODCACHE TMPDIR; do
 value="${!variable:-}"
 if [[ "$value" != /* || ! -d "$value" ]]; then echo 'Declare run-owned absolute cache/temp directories' >&2; exit 64; fi
done
export GOTOOLCHAIN=local GOMAXPROCS=4
scratch="$(mktemp -d "$TMPDIR/source-verification.XXXXXX")"
real_docker="$(type -P docker)"
image='gcr.io/cloud-spanner-emulator/emulator@sha256:c6f3402f2599684f295a0fdefb6fbbbfb18a0e43e309ff5456ccb452a4570a79'
image_owned=false
cleanup(){
 local rc=$? failed=0
 trap - EXIT
 if [[ "$image_owned" == true ]]; then
  timeout 30 "$real_docker" --context "$VELENZA_VERIFY_DOCKER_CONTEXT" image rm "$image" || failed=1
  if "$real_docker" --context "$VELENZA_VERIFY_DOCKER_CONTEXT" image inspect "$image" >/dev/null 2>&1; then failed=1; fi
 fi
 rm -rf -- "$scratch"
 if [[ -e "$scratch" ]]; then failed=1; fi
 if [[ "$failed" != 0 ]]; then echo 'Owned verification cleanup failed' >&2; exit 1; fi
 echo 'Owned verification outputs and emulator cleaned'
 exit "$rc"
}
trap cleanup EXIT
# The caller records this stdout against the frozen source revision before its
# uniquely named run workspace/caches are removed by the outer run trap.
git diff --quiet
git diff --cached --quiet
if [[ -n "$(git ls-files --others --exclude-standard)" ]]; then echo 'Frozen source must not contain untracked files' >&2; exit 1; fi
printf 'SOURCE_REVISION %s\n' "$(git rev-parse HEAD)"
go version
sh -n scripts/provider-documentation-factory-release.sh 2>/dev/null || { [[ ! -e scripts/provider-documentation-factory-release.sh ]] || exit 1; }
go vet ./...
go build ./...
go test -race -p 2 -count=1 -timeout=30m -coverprofile="$scratch/unit.cover" -json ./... | tee "$scratch/unit.json"
go tool cover -func="$scratch/unit.cover"
python3 - "$scratch/unit.json" <<'PY'
import json,sys
rows=[json.loads(x) for x in open(sys.argv[1])]
print('UNIT_TEST_PASS_COUNT',sum(x.get('Action')=='pass' and 'Test' in x for x in rows))
PY
# Reserve two CPUs and two GiB for Docker, which runs outside the caller's
# cgroup. The remaining test/build descendants keep a hard six-GiB/two-CPU cap.
systemctl --user set-property --runtime "$scope" MemoryMax=6G CPUQuota=200% MemorySwapMax=0
export GOMAXPROCS=2 GOMEMLIMIT=4GiB
mkdir "$scratch/bin"
export VELENZA_VERIFY_DOCKER="$real_docker"
cat > "$scratch/bin/docker" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == container && "${2:-}" == create ]]; then
 shift 2
 exec "$VELENZA_VERIFY_DOCKER" --context "$VELENZA_VERIFY_DOCKER_CONTEXT" container create --cpus=2 --memory=2g --memory-swap=2g --pids-limit=256 --log-driver=local --log-opt=max-size=10m --log-opt=max-file=2 "$@"
fi
exec "$VELENZA_VERIFY_DOCKER" --context "$VELENZA_VERIFY_DOCKER_CONTEXT" "$@"
SH
chmod 700 "$scratch/bin/docker"
export PATH="$scratch/bin:$PATH"
if ! "$real_docker" --context "$VELENZA_VERIFY_DOCKER_CONTEXT" image inspect "$image" >/dev/null 2>&1; then image_owned=true; fi
timeout 1200 bash ./scripts/test-integration.sh
